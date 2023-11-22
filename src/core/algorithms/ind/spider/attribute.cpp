/** \file
 * \brief Spider attribute
 *
 * Attribute class methods definition
 */
#include "attribute.h"

namespace algos::spider {

Attribute::Attribute(AttributeIndex attr_id, AttributeIndex attr_count,
                     model::ColumnDomain const& domain)
    : id_(attr_id), it_(domain) {
    for (AttributeIndex i = 0; i != attr_count; ++i) {
        if (id_ == i) continue;
        refs_.insert(i);
        deps_.insert(i);
    }
}

void Attribute::IntersectRefs(std::unordered_set<AttributeIndex> const& ids,
                              std::vector<Attribute>& attrs) {
    for (auto it = refs_.begin(); it != refs_.end();) {
        auto ref_id = *it;
        if (ids.find(ref_id) == ids.end()) {
            it = refs_.erase(it);
            attrs[ref_id].RemoveDependent(GetId());
        } else {
            it++;
        }
    }
}

int Attribute::Compare(Attribute const& lhs, Attribute const& rhs) {
    int cmp = lhs.GetIt().GetValue().compare(rhs.GetIt().GetValue());
    if (cmp == 0) {
        // FIXME: use `std::compare(lhs.GetId(), rhs.GetId())` in C++20
        cmp = (lhs.GetId() > rhs.GetId()) - (lhs.GetId() < rhs.GetId());
    }
    return cmp;
}

}  // namespace algos::spider
