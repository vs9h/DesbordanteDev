/** \file
 * \brief Column domain data iterator
 *
 * implementation of the ColumnDomainIterator class, which provides column domain values
 */
#pragma once

#include <memory>
#include <queue>
#include <vector>

#include "column_domain.h"

namespace model {

/// iterator for column domain data
class ColumnDomainIterator {
private:
    using Reader = DomainPartition::PartitionReader;
    using Value = Reader::Value;

    static bool ReaderGreater(Reader const* lhs, Reader const* rhs) {
        return lhs->GetValue() > rhs->GetValue();
    }
    using ReaderPQ = std::priority_queue<Reader*, std::vector<Reader*>, decltype(&ReaderGreater)>;

    ColumnDomain const& domain_;
    std::vector<std::unique_ptr<Reader>> readers_;
    ReaderPQ readers_pq_;
    Value value_;

    static std::vector<std::unique_ptr<Reader>> CreateReaders(
            ColumnDomain::RawData const& partitions);
    static ReaderPQ CreateReadersPQ(std::vector<std::unique_ptr<Reader>> const& readers);

public:
    ColumnDomainIterator(ColumnDomain const& domain);

    ColumnDomain const& GetDomain() const {
        return domain_;
    }

    void MoveToNext();
    bool TryMove();

    bool HasNext() const {
        return !readers_pq_.empty();
    }
    Value const& GetValue() const {
        return value_;
    }
};

}  // namespace model
