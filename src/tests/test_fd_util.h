#pragma once

#include <filesystem>

#include <gtest/gtest.h>

#include "algorithms/algo_factory.h"
#include "algorithms/fd/fd_algorithm.h"
#include "all_datasets_info.h"
#include "config/error/type.h"
#include "config/names.h"
#include "datasets.h"

template <typename T>
class AlgorithmTest : public ::testing::Test {
    static config::InputTable MakeCsvParser(std::string const& path, char separator,
                                            bool has_header) {
        return std::make_shared<CSVParser>(path, separator, has_header);
    }

protected:
    static std::unique_ptr<algos::FDAlgorithm> CreateAndConfToLoad(std::string const& path,
                                                                   char separator = ',',
                                                                   bool has_header = true) {
        using config::InputTable, algos::ConfigureFromMap, algos::StdParamsMap;
        std::unique_ptr<algos::FDAlgorithm> algorithm = std::make_unique<T>();
        auto parser = MakeCsvParser(path, separator, has_header);
        ConfigureFromMap(*algorithm, StdParamsMap{{config::names::kTable, parser}});
        return algorithm;
    }

    static algos::StdParamsMap GetParamMap(const std::filesystem::path& path, char separator = ',',
                                           bool has_header = true) {
        using namespace config::names;
        return {
                {kTable, MakeCsvParser(path, separator, has_header)},
                {kError, config::ErrorType{0.0}},
                {kSeed, decltype(pyro::Parameters::seed){0}},
        };
    }

public:
    static std::unique_ptr<algos::FDAlgorithm> CreateAlgorithmInstance(const std::string& filename,
                                                                       char separator = ',',
                                                                       bool has_header = true) {
        return algos::CreateAndLoadAlgorithm<T>(
                GetParamMap(test_data_dir / filename, separator, has_header));
    }

    static inline const std::vector<tests::DatasetHashPair> light_datasets_ = {
            {{tests::kCIPublicHighway10k, 33398},
             {tests::kneighbors10k, 43368},
             {tests::kWDC_astronomical, 22281},
             {tests::kWDC_age, 19620},
             {tests::kWDC_appearances, 25827},
             {tests::kWDC_astrology, 40815},
             {tests::kWDC_game, 6418},
             {tests::kWDC_science, 19620},
             {tests::kWDC_symbols, 28289},
             {tests::kbreast_cancer, 15121},
             {tests::kWDC_kepler, 63730}}};

    static inline const std::vector<tests::DatasetHashPair> heavy_datasets_ = {
            {{tests::kadult, 23075},
             {tests::kCIPublicHighway, 13035},
             {tests::kEpicMeds, 50218},
             {tests::kEpicVitals, 2083},
             {tests::kiowa1kk, 28573},
             {tests::kLegacyPayors, 43612}}};
};
