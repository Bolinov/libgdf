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
#include <typeinfo>
#include <gdf/parquet/column_reader.h>

#include "dictionary_decoder.h"
#include "plain_decoder.h"

namespace gdf {
namespace parquet {

template <class DataType, class DecoderType>
static inline void
_ConfigureDictionary(
  const ::parquet::Page *                                page,
  std::unordered_map<int, std::shared_ptr<DecoderType>> &decoders,
  const ::parquet::ColumnDescriptor *const               column_descriptor,
  ::arrow::MemoryPool *const                               pool,
  DecoderType **                                         out_decoder) {
    const ::parquet::DictionaryPage *dictionary_page =
      static_cast<const ::parquet::DictionaryPage *>(page);

    int encoding = static_cast<int>(dictionary_page->encoding());
    if (dictionary_page->encoding() == ::parquet::Encoding::PLAIN_DICTIONARY
        || dictionary_page->encoding() == ::parquet::Encoding::PLAIN) {
        encoding = static_cast<int>(::parquet::Encoding::RLE_DICTIONARY);
    }

    auto it = decoders.find(encoding);
    if (it != decoders.end()) {
        throw ::parquet::ParquetException(
          "Column cannot have more than one dictionary.");
    }

    if (dictionary_page->encoding() == ::parquet::Encoding::PLAIN_DICTIONARY
        || dictionary_page->encoding() == ::parquet::Encoding::PLAIN) {
        internal::PlainDecoder<DataType> dictionary(column_descriptor);
        dictionary.SetData(
          dictionary_page->num_values(), page->data(), page->size());
        
        // std::cout << "datatype: " << typeid(DataType).name() << std::endl;
        // std::cout << "datatype: " << typeid(::parquet::Int32Type).name() << std::endl;
        if (typeid(DataType) == typeid(::parquet::Int32Type) ) {
            auto decoder = std::make_shared<internal::DictionaryDecoder<DataType, gdf::arrow::internal::RleDecoder>>(
            column_descriptor, pool);
            decoder->SetDict(&dictionary);
            decoders[encoding] = decoder;
        } else {
            auto decoder = std::make_shared<internal::DictionaryDecoder<DataType, ::arrow::RleDecoder>>(
            column_descriptor, pool);
            decoder->SetDict(&dictionary);
            decoders[encoding] = decoder;
        }
    } else {
        ::parquet::ParquetException::NYI(
          "only plain dictionary encoding has been implemented");
    }

    *out_decoder = decoders[encoding].get();
}

static inline bool
_IsDictionaryIndexEncoding(const ::parquet::Encoding::type &e) {
    return e == ::parquet::Encoding::RLE_DICTIONARY
           || e == ::parquet::Encoding::PLAIN_DICTIONARY;
}

template <class DecoderType, class T>
static inline std::int64_t
_ReadValues(DecoderType *decoder, std::int64_t batch_size, T *out) {
    std::int64_t num_decoded =
      decoder->Decode(out, static_cast<int>(batch_size));
    return num_decoded;
}

template <class DataType>
bool
ColumnReader<DataType>::HasNext() {
    if (num_buffered_values_ == 0
        || num_decoded_values_ == num_buffered_values_) {
        if (!ReadNewPage() || num_buffered_values_ == 0) { return false; }
    }
    return true;
}

template <class DataType>
bool
ColumnReader<DataType>::ReadNewPage() {
    const std::uint8_t *buffer;

    for (;;) {
        current_page_ = pager_->NextPage();
        if (!current_page_) { return false; }

        if (current_page_->type() == ::parquet::PageType::DICTIONARY_PAGE) {
            _ConfigureDictionary<DataType>(current_page_.get(),
                                           decoders_,
                                           descr_,
                                           pool_,
                                           &current_decoder_);
            continue;
        } else if (current_page_->type() == ::parquet::PageType::DATA_PAGE) {
            const ::parquet::DataPage *page =
              static_cast<const ::parquet::DataPage *>(current_page_.get());

            num_buffered_values_ = page->num_values();
            num_decoded_values_  = 0;
            buffer               = page->data();

            std::int64_t data_size = page->size();

            if (descr_->max_repetition_level() > 0) {
                std::int64_t rep_levels_bytes =
                  repetition_level_decoder_.SetData(
                    page->repetition_level_encoding(),
                    descr_->max_repetition_level(),
                    static_cast<int>(num_buffered_values_),
                    buffer);
                buffer += rep_levels_bytes;
                data_size -= rep_levels_bytes;
            }

            if (descr_->max_definition_level() > 0) {
                std::int64_t def_levels_bytes =
                  definition_level_decoder_.SetData(
                    page->definition_level_encoding(),
                    descr_->max_definition_level(),
                    static_cast<int>(num_buffered_values_),
                    buffer);
                buffer += def_levels_bytes;
                data_size -= def_levels_bytes;
            }

            ::parquet::Encoding::type encoding = page->encoding();

            if (_IsDictionaryIndexEncoding(encoding)) {
                encoding = ::parquet::Encoding::RLE_DICTIONARY;
            }

            auto it = decoders_.find(static_cast<int>(encoding));
            if (it != decoders_.end()) {
                if (encoding == ::parquet::Encoding::RLE_DICTIONARY) {
                    DCHECK(current_decoder_->encoding()
                           == ::parquet::Encoding::RLE_DICTIONARY);
                }
                current_decoder_ = it->second.get();
            } else {
                switch (encoding) {
                case ::parquet::Encoding::PLAIN: {
                    std::shared_ptr<DecoderType> decoder(
                      new internal::PlainDecoder<DataType>(descr_));
                    decoders_[static_cast<int>(encoding)] = decoder;
                    current_decoder_                      = decoder.get();
                    break;
                }
                case ::parquet::Encoding::RLE_DICTIONARY:
                    throw ::parquet::ParquetException(
                      "Dictionary page must be before data page.");

                case ::parquet::Encoding::DELTA_BINARY_PACKED:
                case ::parquet::Encoding::DELTA_LENGTH_BYTE_ARRAY:
                case ::parquet::Encoding::DELTA_BYTE_ARRAY:
                    ::parquet::ParquetException::NYI("Unsupported encoding");

                default:
                    throw ::parquet::ParquetException(
                      "Unknown encoding type.");
                }
            }
            current_decoder_->SetData(static_cast<int>(num_buffered_values_),
                                      buffer,
                                      static_cast<int>(data_size));
            return true;
        } else {
            continue;
        }
    }
    return true;
}

template <class DataType>
inline std::int64_t
ColumnReader<DataType>::ReadBatch(std::int64_t  batch_size,
                                  std::int16_t *def_levels,
                                  std::int16_t *rep_levels,
                                  T *           values,
                                  std::int64_t *values_read) {
    if (!HasNext()) {
        *values_read = 0;
        return 0;
    }

    batch_size =
      std::min(batch_size, num_buffered_values_ - num_decoded_values_);

    std::int64_t num_def_levels = 0;
    std::int64_t num_rep_levels = 0;

    std::int64_t values_to_read = 0;

    if (descr_->max_definition_level() > 0 && def_levels) {
        num_def_levels = ReadDefinitionLevels(batch_size, def_levels);
        for (std::int64_t i = 0; i < num_def_levels; ++i) {
            if (def_levels[i] == descr_->max_definition_level()) {
                ++values_to_read;
            }
        }
    } else {
        values_to_read = batch_size;
    }

    if (descr_->max_repetition_level() > 0 && rep_levels) {
        num_rep_levels = ReadRepetitionLevels(batch_size, rep_levels);
        if (def_levels && num_def_levels != num_rep_levels) {
            throw ::parquet::ParquetException(
              "Number of decoded rep / def levels did not match");
        }
    }

    *values_read = _ReadValues(current_decoder_, values_to_read, values);
    std::int64_t total_values = std::max(num_def_levels, *values_read);
    ConsumeBufferedValues(total_values);

    return total_values;
}

template <class DataType>
struct ParquetTraits {};

#define TYPE_TRAITS_FACTORY(ParquetType, GdfDType)                            \
    template <>                                                               \
    struct ParquetTraits<ParquetType> {                                       \
        static constexpr gdf_dtype gdfDType = GdfDType;                       \
    }

TYPE_TRAITS_FACTORY(::parquet::BooleanType, GDF_invalid);
TYPE_TRAITS_FACTORY(::parquet::Int32Type, GDF_INT32);
TYPE_TRAITS_FACTORY(::parquet::Int64Type, GDF_INT64);
TYPE_TRAITS_FACTORY(::parquet::Int96Type, GDF_invalid);
TYPE_TRAITS_FACTORY(::parquet::FloatType, GDF_FLOAT32);
TYPE_TRAITS_FACTORY(::parquet::DoubleType, GDF_FLOAT64);
TYPE_TRAITS_FACTORY(::parquet::ByteArrayType, GDF_invalid);
TYPE_TRAITS_FACTORY(::parquet::FLBAType, GDF_invalid);
#undef TYPE_TRAITS_FACTORY

static inline std::uint8_t
_ByteWithBit(std::ptrdiff_t i) {
    return (std::uint8_t[]){1, 2, 4, 8, 16, 32, 64, 128}[i];
}

static inline void
_TurnBitOn(std::uint8_t *const bits, std::ptrdiff_t i) {
    bits[i / 8] |= _ByteWithBit(i % 8);
}

static inline std::size_t
_CeilToByteLength(std::size_t n) {
    return (n + 7) & ~7;
}

static inline std::size_t
_BytesLengthToBitmapLength(std::size_t n) {
    return _CeilToByteLength(n) / 8;
}

static inline std::size_t
_GenerateNullBitmap(const std::int16_t *const levels,
                    const std::size_t         levels_length,
                    const std::int16_t        file_level,
                    std::uint8_t *const       null_bitmap_ptr) {
    std::size_t null_count = 0;

    for (std::size_t i = 0; i < levels_length; i++) {
        if (levels[i] == file_level) {
            _TurnBitOn(null_bitmap_ptr, i);
        } else {
            DCHECK_LT(levels[i], file_level);
            null_count += 1;
        }
    }

    return null_count;
}

template <class DataType>
std::size_t
ColumnReader<DataType>::ReadGdfColumn(std::size_t values_to_read,
                                      std::shared_ptr<gdf_column> *out) {
    constexpr std::size_t type_size = static_cast<std::size_t>(
      ::parquet::type_traits<DataType::type_num>::value_byte_size);

    std::int64_t values_read;

    std::int16_t *levels = new std::int16_t[values_to_read];

    gdf_column *column = new gdf_column;

    column->data = new std::uint8_t[type_size * values_to_read];

    ReadBatch(static_cast<std::int64_t>(values_to_read),
              levels,
              nullptr,
              static_cast<T *>(column->data),
              &values_read);

    std::size_t levels_length = _BytesLengthToBitmapLength(values_read);
    column->valid             = new std::uint8_t[levels_length];
    _GenerateNullBitmap(
      levels, values_read, descr_->max_definition_level(), column->valid);

    column->size  = static_cast<gdf_size_type>(values_read);
    column->dtype = ParquetTraits<DataType>::gdfDType;

    out->reset(column);

    return static_cast<std::size_t>(values_read);
}

template class ColumnReader<::parquet::BooleanType>;
template class ColumnReader<::parquet::Int32Type>;
template class ColumnReader<::parquet::Int64Type>;
//template class ColumnReader<::parquet::Int96Type>;
template class ColumnReader<::parquet::FloatType>;
template class ColumnReader<::parquet::DoubleType>;
// template class ColumnReader<::parquet::ByteArrayType>;
// template class ColumnReader<::parquet::FLBAType>;

}  // namespace parquet
}  // namespace gdf