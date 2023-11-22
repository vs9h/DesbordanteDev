#include "config/mem_limit/option.h"

#include "config/names_and_descriptions.h"

namespace config {
using names::kMemLimitMB, descriptions::kDMemLimitMB;
extern const CommonOption<MemLimitMBType> MemLimitMBOpt{
        kMemLimitMB, kDMemLimitMB, 2048, [](auto &value) {
            MemLimitMBType min_limit_mb = 16u;
            if (value < min_limit_mb) {
                throw ConfigurationError("Memory limit must be at least " + std::to_string(value) +
                                         "MB");
            }
        }};
}  // namespace config
