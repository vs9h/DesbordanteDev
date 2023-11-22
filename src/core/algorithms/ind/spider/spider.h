/** \file
 * \brief Spider algorithm
 *
 * Spider algorithm class definition
 */
#pragma once
#include <vector>

#include "algorithms/ind/ind_algorithm.h"
#include "attribute.h"
#include "config/mem_limit/type.h"
#include "config/thread_number/type.h"
#include "model/table/column_domain.h"

namespace algos {

///
/// \brief disk-based unary inclusion dependency mining algorithm
///
/// \note modification: writing to disk is only triggered
///       when the algorithm encounters memory limit
///
class Spider final : public INDAlgorithm {
public:
    /// timing information for algorithm stages
    struct StageTimings {
        size_t load;    /**< time taken for the data loading */
        size_t init;    /**< time taken for the attributes initialization */
        size_t compute; /**< time taken for the inds computing */
        size_t total;   /**< total time taken for all stages */
    };

private:
    using Domain = model::ColumnDomain;
    using Attribute = spider::Attribute;

    /* configuration stage fields */
    config::ThreadNumType threads_num_;
    config::MemLimitMBType mem_limit_mb_;

    /* execution stage fields */
    std::vector<Domain> domains_;  /*< loaded data */
    std::vector<Attribute> attrs_; /*< collection of attributes considered at execution stage */
    StageTimings timings_;         /*< timings info */

    void MakeLoadOptsAvailable();
    void LoadDataInternal() final;

    /* fill vector of attributes */
    void InitAttributes();
    /* process all attributes and prune candidates */
    void ProcessAttributes();
    /* register all dependencies */
    void RegisterDeps();

    unsigned long long ExecuteInternal() final;
    void ResetINDAlgorithmState() final;

public:
    explicit Spider();

    /// get information about stage timings
    StageTimings const& GetStageTimings() const {
        return timings_;
    }
};

}  // namespace algos
