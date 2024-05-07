#include "algorithms/md/hymd/preprocessing/similarity_measure/distance_similarity_measure.h"
#include "algorithms/md/hymd/similarity_measure_creator.h"
#include "config/exceptions.h"

double NumberDifference(model::Double left, model::Double right) {
    return std::abs(left - right);
}

namespace algos::hymd::preprocessing::similarity_measure {
class NumberSimilarityMeasure : public DistanceSimilarityMeasure {
    static constexpr auto kName = "number_similarity";

public:
    class Creator final : public SimilarityMeasureCreator {
        model::md::DecisionBoundary const min_sim_;

    public:
        Creator(model::md::DecisionBoundary min_sim)
            : SimilarityMeasureCreator(kName), min_sim_(min_sim) {
            if (!(0.0 <= min_sim_ && min_sim_ <= 1.0)) {
                throw config::ConfigurationError("Minimum similarity out of range");
            }
        }

        std::unique_ptr<SimilarityMeasure> MakeMeasure() const final {
            return std::make_unique<NumberSimilarityMeasure>(min_sim_);
        }
    };

public:
    NumberSimilarityMeasure(model::md::DecisionBoundary min_sim)
        : DistanceSimilarityMeasure(
                  std::make_unique<model::DoubleType>(),
                  [](std::byte const* l, std::byte const* r) {
                      model::Double left_val = model::Type::GetValue<model::Double>(l);
                      model::Double right_val = model::Type::GetValue<model::Double>(r);
                      return NumberDifference(left_val, right_val);
                  },
                  min_sim) {}
};
}  // namespace algos::hymd::preprocessing::similarity_measure