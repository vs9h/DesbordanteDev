/** \file
 * \brief Block data
 *
 * BlockData and ColumnData classes methods definition
 */
#include "block_data.h"

#include <cassert>
#include <limits>

namespace model {

namespace details {

using ColumnIterator = ColumnData::ColumnIterator;

void ColumnIterator::ConstructView() {
    Offset end = (std::next(cur_) != end_) ? *std::next(cur_) : buffer_.size();
    char const* str_data = buffer_.data() + *cur_;
    size_t str_size = end - *cur_;
    view_ = {str_data, str_size};
}

ColumnIterator::ColumnIterator(std::vector<char> const& data, std::vector<Offset> const& offsets)
    : buffer_(data), cur_(offsets.cbegin()), end_(offsets.cend()) {
    assert(!offsets.empty());
    ConstructView();
}

bool ColumnIterator::TryMoveToNext() {
    bool can_move = std::next(cur_) != end_;
    if (can_move) {
        std::advance(cur_, 1);
        ConstructView();
    }
    return can_move;
}

void ColumnData::Insert(std::string const& str) {
    assert(raw_data_.size() < std::numeric_limits<Offset>::max());
    Offset offset = raw_data_.size();
    std::copy(str.begin(), str.end(), std::back_inserter(raw_data_));
    offsets_.push_back(offset);
}

}  // namespace details

void BlockData::InsertRow(std::vector<std::string>& row) {
    for (size_t i = 0; i != row.size(); ++i) {
        columns_[i].Insert(row[i]);
    }
}

}  // namespace model
