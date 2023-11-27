#pragma once

#include <filesystem>
#include <string>
#include <vector>

static const auto test_data_dir = std::filesystem::current_path() / "input_data";

namespace tests {

/// dataset configuration info to create an input table
struct DatasetInfo {
    std::string name;
    char separator;
    bool has_header;
};

/// a pair consisting of a dataset and the expected hash
using DatasetHashPair = std::pair<DatasetInfo, size_t>;
/// a pair consisting of a vector of datasets and an expected hash
using DatasetsHashPair = std::pair<std::vector<DatasetInfo>, size_t>;

}  // namespace tests
