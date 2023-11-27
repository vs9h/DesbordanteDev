#include <filesystem>

#include <gtest/gtest.h>

#include "algorithms/algo_factory.h"
#include "all_datasets_info.h"
#include "config/names.h"
#include "config/thread_number/type.h"
#include "datasets.h"
#include "test_hash_util.h"

namespace fs = std::filesystem;
using namespace algos;

namespace tests {

namespace {
template <typename Algorithm>
class INDAlgorithmTest : public ::testing::Test {
    static config::ThreadNumType threads_;

    static StdParamsMap GetParamMap(config::InputTables const& input_tables) {
        using namespace config::names;
        return {{kTables, input_tables}, {kThreads, threads_}};
    }

protected:
    static void SetThreadsParam(config::ThreadNumType threads) noexcept {
        assert(threads > 0);
        threads_ = threads;
    }

public:
    template <typename... Args>
    static std::unique_ptr<Algorithm> CreateAlgorithmInstance(Args&&... args) {
        return CreateAndLoadAlgorithm<Algorithm>(GetParamMap(std::forward<Args>(args)...));
    }

    /* hashes for light datasets (unary deps) */
    static inline const std::vector<DatasetsHashPair> light_datasets_ = {
            {{kWDC_astronomical}, 1U},
            {{kWDC_symbols}, 1U},
            {{kWDC_science}, 1U},
            {{kWDC_satellites}, 1U},
            {{kWDC_appearances}, 1U},
            {{kWDC_astrology}, 13455143437649811744U},
            {{kWDC_game}, 447511263452U},
            {{kWDC_kepler}, 1U},
            {{kWDC_planetz}, 1U},
            {{kWDC_age}, 1U},
            {{kTestWide}, 7112674290840U},
            {{kabalone}, 11213732566U},
            {{kiris}, 1U},
            {{kadult}, 118907247627U},
            {{kbreast_cancer}, 1U},
            {{kneighbors10k}, 139579476277123U},
            {{kneighbors100k}, 139579476277123U},
            {{kCIPublicHighway10k}, 3501995834407208U},
            {{kCIPublicHighway700}, 6532935312084701U}};

    /* hashes for heavy datasets (unary deps) */
    static inline const std::vector<DatasetsHashPair> heavy_datasets_ = {
            {{kEpicVitals}, 8662177202540121819U},
            {{kEpicMeds}, 882145071506841335U},
            {{kiowa1kk}, 232519218595U}};
};

template <typename Algorithm>
config::ThreadNumType INDAlgorithmTest<Algorithm>::threads_ = 1;

template <typename T>
void PerformConsistentHashTestOn(std::vector<DatasetsHashPair> const& datasets_hash) {
    using CCTest = std::pair<model::TableIndex, std::vector<model::ColumnIndex>>;
    using INDTest = std::pair<CCTest, CCTest>;
    auto dataset_names_to_string = [](std::vector<DatasetInfo> const& datasets) {
        std::stringstream ss;
        for (DatasetInfo const& dataset : datasets) {
            ss << dataset.name << " ";
        }
        return ss.str();
    };

    auto to_ind_test = [](model::IND const& ind) {
        model::ColumnCombination lhs = ind.GetLhs();
        auto to_cc_test = [](model::ColumnCombination const& cc) {
            return std::make_pair(cc.GetTableIndex(), cc.GetColumnIndices());
        };
        return std::make_pair(to_cc_test(ind.GetLhs()), to_cc_test(ind.GetRhs()));
    };
    for (auto const& [datasets, hash] : datasets_hash) {
        try {
            config::InputTables input_tables;
            input_tables.reserve(datasets.size());
            for (DatasetInfo const& dataset : datasets) {
                input_tables.push_back(std::make_shared<CSVParser>(
                        test_data_dir / dataset.name, dataset.separator, dataset.has_header));
            }
            auto ind_algo = T::CreateAlgorithmInstance(input_tables);
            ind_algo->Execute();
            std::list<model::IND> const& actual_list = ind_algo->INDList();
            std::vector<INDTest> actual;
            actual.reserve(actual_list.size());
            std::transform(actual_list.begin(), actual_list.end(), std::back_inserter(actual),
                           to_ind_test);
            std::sort(actual.begin(), actual.end());
            EXPECT_EQ(HashVec(actual, HashPair), hash)
                    << "Wrong hash on datasets " << dataset_names_to_string(datasets);
        } catch (std::exception const& e) {
            std::cerr << "An exception with message: " << e.what() << "\n\tis thrown on datasets "
                      << dataset_names_to_string(datasets) << '\n';
            FAIL();
        }
    }
}
}  // namespace

TYPED_TEST_SUITE_P(INDAlgorithmTest);

TYPED_TEST_P(INDAlgorithmTest, ConsistentHashOnLightDatasets) {
    TestFixture::SetThreadsParam(1);
    PerformConsistentHashTestOn<TestFixture>(TestFixture::light_datasets_);
}

TYPED_TEST_P(INDAlgorithmTest, ConsistentHashOnHeavyDatasets) {
    TestFixture::SetThreadsParam(1);
    PerformConsistentHashTestOn<TestFixture>(TestFixture::heavy_datasets_);
}

TYPED_TEST_P(INDAlgorithmTest, ConsistentHashOnLightDatasetsParallel) {
    TestFixture::SetThreadsParam(4);
    PerformConsistentHashTestOn<TestFixture>(TestFixture::light_datasets_);
}

TYPED_TEST_P(INDAlgorithmTest, ConsistentHashOnHeavyDatasetsParallel) {
    TestFixture::SetThreadsParam(4);
    PerformConsistentHashTestOn<TestFixture>(TestFixture::heavy_datasets_);
}

REGISTER_TYPED_TEST_SUITE_P(INDAlgorithmTest, ConsistentHashOnLightDatasets,
                            ConsistentHashOnHeavyDatasets, ConsistentHashOnLightDatasetsParallel,
                            ConsistentHashOnHeavyDatasetsParallel);

using Algorithms = ::testing::Types<algos::Spider>;
INSTANTIATE_TYPED_TEST_SUITE_P(INDAlgorithmTest, INDAlgorithmTest, Algorithms);

}  // namespace tests
