/** \file
 * \brief Block data stream
 *
 * BlockDatasetStream class methods definition
 */
#include "block_dataset_stream.h"

#include <easylogging++.h>

namespace model {

size_t BlockDatasetStream::GetRowSize(const Row& row) {
    size_t size = 0;
    for (std::string const& value : row) {
        size += value.size();
    }
    return size;
}

bool BlockDatasetStream::TryStoreNextRow() {
    if (cur_row_.empty() && stream_->HasNextRow()) {
        cur_row_ = stream_->GetNextRow();
        size_t cols_num = stream_->GetNumberOfColumns();
        if (cur_row_.size() != cols_num) {
            LOG(WARNING) << "Received row with size " << cur_row_.size() << ", but expected "
                         << cols_num;
            cur_row_.clear();
            return TryStoreNextRow();
        }
    }
    return HasNextBlock();
}

BlockData BlockDatasetStream::GetNextBlock() {
    BlockData block{static_cast<ColumnIndex>(stream_->GetNumberOfColumns())};
    size_t block_size = 0;
    do {
        if (!block.isEmpty() && block_size + GetRowSize(cur_row_) > capacity_) {
            break;
        }
        block.InsertRow(cur_row_);
        block_size += GetRowSize(cur_row_);
        cur_row_.clear();
    } while (TryStoreNextRow());
    return block;
}

}  // namespace model
