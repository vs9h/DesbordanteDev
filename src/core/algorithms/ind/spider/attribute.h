/** \file
 * \brief Spider attribute
 *
 * Attribute class definition
 */
#pragma once

#include <unordered_set>
#include <vector>

#include "model/table/column_domain_iterator.h"
#include "model/table/column_index.h"

namespace algos::spider {

using AttributeIndex = model::ColumnIndex;

///
/// \brief class containing an iterator to traverse through attribute domain
///        and holding candidates for UINDs
///
/// \note a bitset can be used to maintain a list of candidates, but this
///       change will not affect the overall performance of the algorithm,
///       since data loading takes many times longer
///
class Attribute {
public:
    using Iterator = model::ColumnDomainIterator;

private:
    AttributeIndex id_;                       /* attribute unique identificator */
    Iterator it_;                             /* domain iterator */
    std::unordered_set<AttributeIndex> refs_; /* referenced attributes indices */
    std::unordered_set<AttributeIndex> deps_; /* dependent attributes indices */

public:
    Attribute(AttributeIndex attr_id, AttributeIndex attr_count, model::ColumnDomain const& domain);

    /// get unqiue attribute id
    AttributeIndex GetId() const {
        return id_;
    }
    /// get attribute iterator
    Iterator& GetIt() {
        return it_;
    }
    /// get attribute iterator
    Iterator const& GetIt() const {
        return it_;
    }
    /// get dependent attributes indices
    std::unordered_set<AttributeIndex> const& GetRefIds() const {
        return refs_;
    }
    /// get referenced attributes indices
    std::unordered_set<AttributeIndex> const& GetDepIds() const {
        return deps_;
    }

    ///
    /// \brief intersect referenced attributes indices with provided indices
    ///
    /// Iterate through the referenced attributes indices and remove those
    /// that do not exist in the provided indices.\n
    /// Additionally, it updates dependent attributes indices.
    ///
    /// \param ids    indices to intersect with
    /// \param attrs  attribute objects to update
    ///
    void IntersectRefs(std::unordered_set<AttributeIndex> const& ids,
                       std::vector<Attribute>& attrs);

    /// remove dependent attribute index
    void RemoveDependent(AttributeIndex id) {
        deps_.erase(id);
    }

    ///
    /// \brief check whether the attribute has processed
    ///
    /// processing is completed if all values have been processed
    /// or there are no more dependent and referenced candidates
    ///
    bool HasFinished() const {
        return !GetIt().HasNext() || (GetRefIds().empty() && GetDepIds().empty());
    }

    /// compare attributes first by their values and then by their ids
    static int Compare(Attribute const& lhs, Attribute const& rhs);
};

}  // namespace algos::spider
