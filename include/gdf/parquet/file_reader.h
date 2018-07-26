/*
 * Copyright 2018 BlazingDB, Inc.
 *     Copyright 2018 Cristhian Alberto Gonzales Castillo <cristhian@blazingdb.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef _GDF_PARQUET_FILE_READER_H
#define _GDF_PARQUET_FILE_READER_H

#include <parquet/file/reader.h>

namespace gdf {
namespace parquet {

class FileReader {
public:
    static std::unique_ptr<FileReader> OpenFile(
      const std::string &                path,
      const ::parquet::ReaderProperties &properties =
        ::parquet::default_reader_properties(),
      const std::shared_ptr<::parquet::FileMetaData> &metadata = nullptr);

    std::shared_ptr<::parquet::RowGroupReader> RowGroup(int i);
    std::shared_ptr<::parquet::FileMetaData>   metadata() const;

private:
    std::unique_ptr<::parquet::ParquetFileReader> parquetFileReader_;
};

}  // namespace parquet
}  // namespace gdf

#endif
