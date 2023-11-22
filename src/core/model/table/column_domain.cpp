/** \file
 * \brief Column domain data
 *
 * implementation of partition reader classes and domain creation manager
 */
#include "column_domain.h"

#include <cmath>

#include <easylogging++.h>

#include "model/table/block_dataset_stream.h"
#include "util/parallel_for.h"

namespace model {

using PartitionReader = DomainPartition::PartitionReader;

/// reader for reading data from main memory
class MemoryBackedReader final : public PartitionReader {
public:
    using Values = DomainPartition::Values;

private:
    using It = Values::iterator;

    It cur_, end_;

public:
    MemoryBackedReader(Values const& values) : cur_(values.begin()), end_(values.end()) {
        assert(cur_ != end_);
    }

    Value const& GetValue() const final {
        return *cur_;
    }
    bool HasNext() const final {
        return std::next(cur_) != end_;
    }
    void MoveToNext() final {
        cur_++;
    }
};

/// reader for reading data from swap file
class FileBackedReader final : public PartitionReader {
private:
    std::ifstream file_;
    Value cur_;

public:
    FileBackedReader(std::filesystem::path const& path) : file_(path) {
        if (!file_.is_open()) {
            throw std::runtime_error("Error opening file");
        }
        MoveToNext();
    }

    Value const& GetValue() const final {
        return cur_;
    }
    bool HasNext() const final {
        return !file_.eof();
    }
    void MoveToNext() final {
        std::getline(file_, cur_);
    }
};

bool PartitionReader::TryMove() {
    bool can_move = HasNext();
    if (can_move) MoveToNext();
    return can_move;
}

DomainPartition::~DomainPartition() {
    if (IsSwapped()) {
        std::filesystem::remove(*swap_file_);
    }
}

std::unique_ptr<PartitionReader> DomainPartition::GetReader() const {
    if (IsSwapped()) {
        return std::make_unique<FileBackedReader>(*swap_file_);
    } else {
        return std::make_unique<MemoryBackedReader>(values_);
    }
}

size_t DomainPartition::GetMemoryUsage() const {
    /* in reality, partition storage takes on average
     * 5 times more memory than the presented estimate
     */
    static constexpr double kAverageOverhead = 5.0;
    size_t total = 0;

    if (IsSwapped()) return total;

    total += values_.size() * sizeof(Values::node_type);

    for (std::string const& str : values_) {
        if (str.size() >= str.capacity()) {
            total += str.capacity();
        }
    }
    return total * kAverageOverhead;
}

bool DomainPartition::TrySwap() {
    namespace fs = std::filesystem;
    if (values_.empty() || IsSwapped()) {
        return false;
    }
    fs::create_directory(kTmpDir);
    fs::path file_path = fs::path{kTmpDir} /
                         (std::to_string(GetTableId()) + "." + std::to_string(GetColumnId()) + "." +
                          std::to_string(GetPartitionId()));
    std::ofstream file{file_path};
    if (!file.is_open()) {
        LOG(ERROR) << "unable to open file for swapping";
        throw std::runtime_error("Cannot open file for swapping");
    }

    for (auto it = values_.begin(); it != values_.end(); ++it) {
        file << *it;
        if (std::next(it) != values_.end()) {
            file << '\n';
        }
    }
    file.close();
    values_.clear();
    swap_file_ = std::make_unique<fs::path>(file_path);
    return true;
}

/// class that contains the logic for creating domains
class DomainManager {
public:
    using Domain = ColumnDomain;
    using DomainVec = std::vector<ColumnDomain>;

private:
    using Partition = DomainPartition;
    using DomainRawData = Domain::RawData;

    size_t mem_limit_;      /* memory limit in bytes */
    size_t block_capacity_; /* optimal block capacity for block datastream */
    size_t mem_usage_;      /* current memory usage in bytes */
    config::ThreadNumType threads_num_;

    DomainVec domains_;                      /* processed domains */
    std::vector<DomainRawData> raw_domains_; /* current table domains */
    size_t processed_block_count_; /* count of processed blocks in current table (reset on swap) */
    size_t swap_candidate_;        /* next domain candidate to swap  */

    /* recalculate memory usage */
    void RefreshMemUsage() {
        mem_usage_ = 0;
        for (DomainRawData const& raw_domain : raw_domains_) {
            /* only last partition isn't swapped */
            mem_usage_ += raw_domain.back().GetMemoryUsage();
        }
        for (Domain const& domain : domains_) {
            mem_usage_ += domain.GetMemoryUsage();
        }
    }

    size_t GetApproximateBlockCount() {
        /* if no block has been processed yet, then we use an estimate of the number of blocks */
        if (processed_block_count_ == 0) {
            auto approx_block_count =
                    static_cast<size_t>(Partition::kMaximumBytesPerChar * block_capacity_);
            return std::max(1UL, mem_limit_ / approx_block_count);
        }
        /* otherwise, use the average amount of memory spent per processed block */
        size_t per_block_mem_usage = mem_usage_ / processed_block_count_;
        return (mem_limit_ - mem_usage_) / per_block_mem_usage;
    }

    /* get the number of blocks for subsequent processing
     * swap to disk if necessary
     */
    size_t GetNumberOfBlocks() {
        RefreshMemUsage();
        size_t block_count = GetApproximateBlockCount();
        while (block_count == 0) {
            SwapNext();
            block_count = GetApproximateBlockCount();
        }
        return block_count;
    }

    /* swap next candidate and refresh memory usage */
    void SwapNext() {
        /* first, try to swap the domain, if there are any */
        if (swap_candidate_ != domains_.size()) {
            Domain& domain = domains_[swap_candidate_];
            size_t domain_mem_usage = domain.GetMemoryUsage();
            domain.Swap();
            mem_usage_ -= domain_mem_usage;
            ++swap_candidate_;
            return;
        }
        /* swap current table domains */
        for (DomainRawData& raw_domain : raw_domains_) {
            Partition& partition = raw_domain.back();
            /* if the partition is empty, then it will not be swapped */
            if (partition.TrySwap()) {
                raw_domain.emplace_back(partition.GetTableId(), partition.GetColumnId(),
                                        partition.GetPartitionId() + 1);
            }
        }
        RefreshMemUsage();
        processed_block_count_ = 0;
    }

    /* process next `block_count` blocks */
    bool ProcessNext(BlockDatasetStream& block_stream, size_t block_count) {
        while (block_count != 0 && block_stream.HasNextBlock()) {
            BlockData block = block_stream.GetNextBlock();
            auto store_column = [&block](DomainRawData& raw_domain) {
                Partition& partition = raw_domain.back();
                auto it = block.GetColumn(partition.GetColumnId()).GetIt();
                do {
                    partition.Insert(std::string{it.GetValue()});
                } while (it.TryMoveToNext());
            };
            util::parallel_foreach(raw_domains_.begin(), raw_domains_.end(), threads_num_,
                                   store_column);
            block_count--;
        }
        return block_stream.HasNextBlock();
    }

    void RawDomainsInit(TableIndex table_id, ColumnIndex col_count) {
        raw_domains_ = std::vector<DomainRawData>(col_count);
        for (ColumnIndex col_id = 0; col_id != col_count; ++col_id) {
            raw_domains_[col_id].emplace_back(table_id, col_id);
        }
    }

    static size_t GetOptimalBlockCapacity(size_t mem_limit_bytes) {
        static constexpr size_t kMaxCapacity = 2 << 20UL;
        /* if the memory limit is 64 MB or more, select a block size of 2 MB
         * otherwise, choose to divide by 32 and find the nearest power of two
         *
         * for example, for 16MB we choose block capacity 512 KB
         */
        size_t capacity = std::pow(2, std::log2(mem_limit_bytes / 32));
        return std::min(capacity, kMaxCapacity);
    }

public:
    DomainManager(size_t mem_limit_mb, config::ThreadNumType threads_num)
        : mem_limit_(mem_limit_mb << 20UL),
          block_capacity_(GetOptimalBlockCapacity(mem_limit_)),
          threads_num_(threads_num),
          swap_candidate_(domains_.size()) {}

    /// create next domain from data stream
    void CreateDomain(TableIndex table_id, std::shared_ptr<model::IDatasetStream> const& stream) {
        auto col_count = static_cast<ColumnIndex>(stream->GetNumberOfColumns());
        RawDomainsInit(table_id, col_count);
        processed_block_count_ = 0;
        size_t block_count = 0;
        BlockDatasetStream block_stream{stream, block_capacity_};
        do {
            processed_block_count_ += block_count;
            block_count = GetNumberOfBlocks();
        } while (ProcessNext(block_stream, block_count));

        for (DomainRawData& raw_domain : raw_domains_) {
            if (!raw_domain.front().IsEmpty()) {
                domains_.push_back(std::move(raw_domain));
            }
        }
        raw_domains_.clear();
    }

    /// get vector of domains
    DomainVec& ReleaseDomains() {
        return domains_;
    }
};

std::vector<ColumnDomain> ColumnDomain::CreateFrom(
        std::vector<std::shared_ptr<model::IDatasetStream>> const& streams,
        config::MemLimitMBType mem_limit_mb, config::ThreadNumType threads_num) {
    TableIndex table_count = static_cast<TableIndex>(streams.size());
    DomainManager manager{mem_limit_mb, threads_num};
    for (TableIndex table_id = 0; table_id != table_count; ++table_id) {
        manager.CreateDomain(table_id, streams[table_id]);
    }
    return std::move(manager.ReleaseDomains());
}

};  // namespace model
