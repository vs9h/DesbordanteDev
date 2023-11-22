/** \file
 * \brief Spider algorithm
 *
 * Spider algorithm class methods definition
 */
#include "spider.h"

#include "config/mem_limit/option.h"
#include "config/names_and_descriptions.h"
#include "config/option_using.h"
#include "config/thread_number/option.h"

namespace algos {

Spider::Spider() : INDAlgorithm({}) {
    DESBORDANTE_OPTION_USING;

    RegisterOption(config::ThreadNumberOpt(&threads_num_));
    RegisterOption(config::MemLimitMBOpt(&mem_limit_mb_));
    MakeLoadOptsAvailable();
}

void Spider::MakeLoadOptsAvailable() {
    MakeOptionsAvailable({config::ThreadNumberOpt.GetName(), config::MemLimitMBOpt.GetName()});
}

namespace {
/* invoke and return the time taken for execution */
template <typename... Ts>
size_t TimedInvoke(Ts&&... invoke_args) {
    using namespace std::chrono;
    auto start_time = system_clock::now();
    std::invoke(std::forward<Ts>(invoke_args)...);
    size_t exec_time = duration_cast<milliseconds>(system_clock::now() - start_time).count();
    return exec_time;
}
}  // namespace

void Spider::LoadDataInternal() {
    auto create_domains = [&] {
        domains_ = Domain::CreateFrom(input_tables_, mem_limit_mb_, threads_num_);
    };
    timings_.load = TimedInvoke(create_domains);
}

void Spider::InitAttributes() {
    using namespace spider;
    AttributeIndex attr_count = domains_.size();
    attrs_.reserve(attr_count);
    for (AttributeIndex attr_id = 0; attr_id != attr_count; ++attr_id) {
        attrs_.emplace_back(attr_id, attr_count, domains_[attr_id]);
    }
}

void Spider::ProcessAttributes() {
    using namespace spider;
    using AttributeRW = std::reference_wrapper<Attribute>;
    auto attr_cmp = [](Attribute const& lhs, Attribute const& rhs) {
        return Attribute::Compare(lhs, rhs) >= 0;
    };
    std::priority_queue<AttributeRW, std::vector<AttributeRW>, decltype(attr_cmp)> attr_pq(
            attrs_.begin(), attrs_.end(), attr_cmp);
    while (!attr_pq.empty()) {
        std::unordered_set<AttributeIndex> ids;
        AttributeRW attr_rw = attr_pq.top();
        std::string const& value = attr_rw.get().GetIt().GetValue();
        do {
            attr_pq.pop();
            ids.insert(attr_rw.get().GetId());
            if (attr_pq.empty()) break;
            attr_rw = attr_pq.top();
        } while (attr_rw.get().GetIt().GetValue() == value);
        for (AttributeIndex id : ids) {
            attrs_[id].IntersectRefs(ids, attrs_);
        }
        for (AttributeIndex id : ids) {
            Attribute& attr = attrs_[id];
            if (!attr.HasFinished()) {
                attr.GetIt().MoveToNext();
                attr_pq.emplace(attr);
            }
        }
    }
}

void Spider::RegisterDeps() {
    auto attr_to_cc = [](Attribute const& attr) {
        Domain const& domain = attr.GetIt().GetDomain();
        return std::make_shared<model::ColumnCombination>(domain.GetTableId(),
                                                          std::vector{domain.GetColumnId()});
    };
    for (Attribute const& dep : attrs_) {
        for (size_t ref_id : dep.GetRefIds()) {
            RegisterIND(attr_to_cc(dep), attr_to_cc(attrs_[ref_id]));
        }
    }
}

unsigned long long Spider::ExecuteInternal() {
    timings_.init = TimedInvoke(&Spider::InitAttributes, this);
    timings_.compute = TimedInvoke(&Spider::ProcessAttributes, this);
    timings_.compute += TimedInvoke(&Spider::RegisterDeps, this);
    timings_.total = timings_.load + timings_.init + timings_.compute;
    return timings_.total;
}

void Spider::ResetINDAlgorithmState() {
    attrs_.clear();
    size_t load = timings_.load;
    timings_ = {};
    timings_.load = load;
}

}  // namespace algos
