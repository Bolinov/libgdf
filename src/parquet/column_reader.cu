/*
 * Copyright 2018 BlazingDB, Inc.
 *     Copyright 2018 Cristhian Alberto Gonzales Castillo <cristhian@blazingdb.com>
 *     Copyright 2018 Alexander Ocsa <alexander@blazingdb.com>
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

#include <arrow/util/bit-util.h>
#include <thrust/device_vector.h>
#include <thrust/execution_policy.h>
#include <thrust/functional.h>
#include <thrust/iterator/counting_iterator.h>
#include <thrust/iterator/discard_iterator.h>
#include <thrust/iterator/permutation_iterator.h>
#include <thrust/iterator/transform_iterator.h>
#include <thrust/device_ptr.h>
#include <thrust/transform.h>

#include "column_reader.h"
#include "dictionary_decoder.cuh"
#include "plain_decoder.cuh"

namespace gdf
{
namespace parquet
{

template <class DataType, class DecoderType>
static inline void
_ConfigureDictionary(
    const ::parquet::Page *page,
    std::unordered_map<int, std::shared_ptr<DecoderType>> &decoders,
    const ::parquet::ColumnDescriptor *const column_descriptor,
    ::arrow::MemoryPool *const pool,
    DecoderType **out_decoder)
{
    const ::parquet::DictionaryPage *dictionary_page =
        static_cast<const ::parquet::DictionaryPage *>(page);

    int encoding = static_cast<int>(dictionary_page->encoding());
    if (dictionary_page->encoding() == ::parquet::Encoding::PLAIN_DICTIONARY || dictionary_page->encoding() == ::parquet::Encoding::PLAIN)
    {
        encoding = static_cast<int>(::parquet::Encoding::RLE_DICTIONARY);
    }

    auto it = decoders.find(encoding);
    if (it != decoders.end())
    {
        throw ::parquet::ParquetException(
            "Column cannot have more than one dictionary.");
    }

    if (dictionary_page->encoding() == ::parquet::Encoding::PLAIN_DICTIONARY || dictionary_page->encoding() == ::parquet::Encoding::PLAIN)
    {
        internal::PlainDecoder<DataType> dictionary(column_descriptor);
        dictionary.SetData(
            dictionary_page->num_values(), page->data(), page->size());

        auto decoder = std::make_shared<internal::DictionaryDecoder<
            DataType, gdf::arrow::internal::RleDecoder>>(column_descriptor, pool);
        decoder->SetDict(&dictionary);
        decoders[encoding] = decoder;
    }
    else
    {
        ::parquet::ParquetException::NYI(
            "only plain dictionary encoding has been implemented");
    }

    *out_decoder = decoders[encoding].get();
}

static inline bool
_IsDictionaryIndexEncoding(const ::parquet::Encoding::type &e)
{
    return e == ::parquet::Encoding::RLE_DICTIONARY || e == ::parquet::Encoding::PLAIN_DICTIONARY;
}

template <class DecoderType, class T>
static inline std::int64_t
_ReadValues(DecoderType *decoder, std::int64_t batch_size, T *out)
{
    std::int64_t num_decoded =
        decoder->Decode(out, static_cast<int>(batch_size));
    return num_decoded;
}

template <class DataType>
bool ColumnReader<DataType>::HasNext()
{
    if (num_buffered_values_ == 0 || num_decoded_values_ == num_buffered_values_)
    {
        if (!ReadNewPage() || num_buffered_values_ == 0)
        {
            return false;
        }
    }
    return true;
}

template <class DataType>
bool ColumnReader<DataType>::ReadNewPage()
{
    const std::uint8_t *buffer;

    for (;;)
    {
        current_page_ = pager_->NextPage();
        if (!current_page_)
        {
            return false;
        }

        if (current_page_->type() == ::parquet::PageType::DICTIONARY_PAGE)
        {
            _ConfigureDictionary<DataType>(current_page_.get(),
                                           decoders_,
                                           descr_,
                                           pool_,
                                           &current_decoder_);
            continue;
        }
        else if (current_page_->type() == ::parquet::PageType::DATA_PAGE)
        {
            const ::parquet::DataPage *page =
                static_cast<const ::parquet::DataPage *>(current_page_.get());

            num_buffered_values_ = page->num_values();
            num_decoded_values_ = 0;
            buffer = page->data();

            std::int64_t data_size = page->size();

            if (descr_->max_repetition_level() > 0)
            {
                std::int64_t rep_levels_bytes =
                    repetition_level_decoder_.SetData(
                        page->repetition_level_encoding(),
                        descr_->max_repetition_level(),
                        static_cast<int>(num_buffered_values_),
                        buffer);
                buffer += rep_levels_bytes;
                data_size -= rep_levels_bytes;
            }

            if (descr_->max_definition_level() > 0)
            {
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

            if (_IsDictionaryIndexEncoding(encoding))
            {
                encoding = ::parquet::Encoding::RLE_DICTIONARY;
            }

            auto it = decoders_.find(static_cast<int>(encoding));
            if (it != decoders_.end())
            {
                if (encoding == ::parquet::Encoding::RLE_DICTIONARY)
                {
                    DCHECK(current_decoder_->encoding() == ::parquet::Encoding::RLE_DICTIONARY);
                }
                current_decoder_ = it->second.get();
            }
            else
            {
                switch (encoding)
                {
                case ::parquet::Encoding::PLAIN:
                {
                    std::shared_ptr<DecoderType> decoder(
                        new internal::PlainDecoder<DataType>(descr_));
                    decoders_[static_cast<int>(encoding)] = decoder;
                    current_decoder_ = decoder.get();
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
        }
        else
        {
            continue;
        }
    }
    return true;
}

static inline bool
_HasSpacedValues(const ::parquet::ColumnDescriptor *descr)
{
    if (descr->max_repetition_level() > 0)
    {
        return !descr->schema_node()->is_required();
    }
    else
    {
        const ::parquet::schema::Node *node = descr->schema_node().get();
        while (node)
        {
            if (node->is_optional())
            {
                return true;
            }
            node = node->parent();
        }
        return false;
    }
}

static inline void
_DefinitionLevelsToBitmap(const std::int16_t *def_levels,
                          std::int64_t num_def_levels,
                          const std::int16_t max_definition_level,
                          const std::int16_t max_repetition_level,
                          std::int64_t *values_read,
                          std::int64_t *null_count,
                          std::uint8_t *valid_bits,
                          const std::int64_t valid_bits_offset)
{
    ::arrow::internal::BitmapWriter valid_bits_writer(
        valid_bits, valid_bits_offset, num_def_levels);

    for (std::int64_t i = 0; i < num_def_levels; ++i)
    {
        if (def_levels[i] == max_definition_level)
        {
            valid_bits_writer.Set();
        }
        else if (max_repetition_level > 0)
        {
            if (def_levels[i] == (max_definition_level - 1))
            {
                valid_bits_writer.Clear();
                *null_count += 1;
            }
            else
            {
                continue;
            }
        }
        else
        {
            if (def_levels[i] < max_definition_level)
            {
                valid_bits_writer.Clear();
                *null_count += 1;
            }
            else
            {
                throw ::parquet::ParquetException(
                    "definition level exceeds maximum");
            }
        }

        valid_bits_writer.Next();
    }
    valid_bits_writer.Finish();
    *values_read = valid_bits_writer.position();
}

template <class DecoderType, class T>
static inline std::int64_t
_ReadValuesSpaced(DecoderType *decoder,
                  std::int64_t batch_size,
                  T *out,
                  std::int64_t null_count,
                  std::uint8_t *valid_bits,
                  std::int64_t valid_bits_offset)
{
    return decoder->DecodeSpaced(out,
                                 static_cast<int>(batch_size),
                                 static_cast<int>(null_count),
                                 valid_bits,
                                 valid_bits_offset);
}

template <typename DataType>
inline std::int64_t
ColumnReader<DataType>::ReadBatchSpaced(std::int64_t batch_size,
                                        std::int16_t *definition_levels,
                                        std::int16_t *repetition_levels,
                                        T *values,
                                        std::uint8_t *valid_bits,
                                        std::int64_t valid_bits_offset, //
                                        std::int64_t *levels_read,
                                        std::int64_t *values_read,
                                        std::int64_t *nulls_count)
{
    if (!HasNext())
    {
        *levels_read = 0;
        *values_read = 0;
        *nulls_count = 0;
        return 0;
    }

    std::int64_t total_values;
    //  num_buffered_values_ - num_decoded_values_

    batch_size =
        std::min(batch_size, num_buffered_values_ - num_decoded_values_);

    if (descr_->max_definition_level() > 0)
    {
        std::int64_t num_def_levels =
            ReadDefinitionLevels(batch_size, definition_levels);

        if (descr_->max_repetition_level() > 0)
        {
            std::int64_t num_rep_levels =
                ReadRepetitionLevels(batch_size, repetition_levels);
            if (num_def_levels != num_rep_levels)
            {
                throw ::parquet::ParquetException(
                    "Number of decoded rep / def levels did not match");
            }
        }

        const bool has_spaced_values = _HasSpacedValues(descr_);

        std::int64_t null_count = 0;
        if (!has_spaced_values)
        {
            int values_to_read = 0;
            for (std::int64_t i = 0; i < num_def_levels; ++i)
            {
                if (definition_levels[i] == descr_->max_definition_level())
                {
                    ++values_to_read;
                }
            }
            std::cout << "*ReadBatchSpaced: before _ReadValues" << std::endl;

            total_values =
                _ReadValues(current_decoder_, values_to_read, values);
            for (std::int64_t i = 0; i < total_values; i++)
            {
                //check: valid_bits_offset + i
                ::arrow::BitUtil::SetBit(valid_bits, valid_bits_offset + i);
            }
            *values_read = total_values;
        }
        else
        {
            std::int16_t max_definition_level = descr_->max_definition_level();
            std::int16_t max_repetition_level = descr_->max_repetition_level();
            _DefinitionLevelsToBitmap(definition_levels,
                                      num_def_levels,
                                      max_definition_level,
                                      max_repetition_level,
                                      values_read,
                                      &null_count,
                                      valid_bits,
                                      valid_bits_offset);

            total_values = _ReadValuesSpaced(current_decoder_,
                                             *values_read,
                                             values,
                                             static_cast<int>(null_count),
                                             valid_bits,
                                             valid_bits_offset);
        }
        *levels_read = num_def_levels;
        *nulls_count = null_count;
    }
    else
    {
        total_values = _ReadValues(current_decoder_, batch_size, values);
        for (std::int64_t i = 0; i < total_values; i++)
        {
            ::arrow::BitUtil::SetBit(valid_bits, valid_bits_offset + i);
        }
        *nulls_count = 0;
        *levels_read = total_values;
    }

    ConsumeBufferedValues(*levels_read);

    return total_values;
}

template <class DataType>
inline std::int64_t
ColumnReader<DataType>::ReadBatch(std::int64_t batch_size,
                                  std::int16_t *def_levels,
                                  std::int16_t *rep_levels,
                                  T *values,
                                  std::int64_t *values_read)
{
    if (!HasNext())
    {
        *values_read = 0;
        return 0;
    }
    batch_size = std::min(batch_size, num_buffered_values_ - num_decoded_values_);

    std::int64_t num_def_levels = 0;
    std::int64_t num_rep_levels = 0;

    std::int64_t values_to_read = 0;

    if (descr_->max_definition_level() > 0 && def_levels)
    {
        num_def_levels = ReadDefinitionLevels(batch_size, def_levels);
        for (std::int64_t i = 0; i < num_def_levels; ++i)
        {
            if (def_levels[i] == descr_->max_definition_level())
            {
                ++values_to_read;
            }
        }
    }
    else
    {
        values_to_read = batch_size;
    }

    if (descr_->max_repetition_level() > 0 && rep_levels)
    {
        num_rep_levels = ReadRepetitionLevels(batch_size, rep_levels);
        if (def_levels && num_def_levels != num_rep_levels)
        {
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
struct ParquetTraits
{
};

#define TYPE_TRAITS_FACTORY(ParquetType, GdfDType)      \
    template <>                                         \
    struct ParquetTraits<ParquetType>                   \
    {                                                   \
        static constexpr gdf_dtype gdfDType = GdfDType; \
    }

TYPE_TRAITS_FACTORY(::parquet::BooleanType, GDF_invalid);
TYPE_TRAITS_FACTORY(::parquet::Int32Type, GDF_INT32);
TYPE_TRAITS_FACTORY(::parquet::Int64Type, GDF_INT64);
TYPE_TRAITS_FACTORY(::parquet::FloatType, GDF_FLOAT32);
TYPE_TRAITS_FACTORY(::parquet::DoubleType, GDF_FLOAT64);

#undef TYPE_TRAITS_FACTORY

//#define TO_GDF_COLUMN_USING_READBATCHSPACED 1

#ifdef TO_GDF_COLUMN_USING_READBATCHSPACED
template <class DataType>
std::size_t
ColumnReader<DataType>::ToGdfColumn(std::int16_t *const definition_levels,
                                    std::int16_t *const repetition_levels,
                                    const gdf_column &column)
{
    typename DataType::c_type *const values =
        static_cast<typename DataType::c_type *const>(column.data);
    std::uint8_t *const valid_bits = static_cast<std::uint8_t *>(column.valid);

    static std::int64_t levels_read = 0;
    static std::int64_t values_read = 0;
    static std::int64_t nulls_count = 0;

    static const std::size_t min_batch_size = 4096;
    std::size_t batch = 0;
    std::size_t batch_actual = 0;
    std::size_t batch_size = 8;
    std::size_t total_read = 0;

    do
    {
        batch = ReadBatchSpaced(
            batch_size,
            definition_levels,
            repetition_levels,
            values + batch_actual,
            valid_bits + static_cast<std::ptrdiff_t>(batch_actual / 8),
            0,
            &levels_read,
            &values_read,
            &nulls_count);
        total_read += static_cast<std::size_t>(values_read);
        batch_actual += batch;
        batch_size = std::max(batch_size * 2, min_batch_size);
    } while (batch > 0 || levels_read > 0);

    return total_read;
}
#else

static inline __device__ __host__ uint8_t _ByteWithBit(ptrdiff_t i)
{
    static uint8_t kBitmask[8] = {1, 2, 4, 8, 16, 32, 64, 128};
    return kBitmask[i];
}

static inline __device__ __host__ uint8_t _FlippedBitmask(ptrdiff_t i)
{
    static uint8_t kFlippedBitmask[] = {254, 253, 251, 247, 239, 223, 191, 127};
    return kFlippedBitmask[i];

}

static inline __device__ __host__ void _TurnBitOn(uint8_t *const bits, std::ptrdiff_t i)
{
    bits[i / 8] |= _ByteWithBit(i % 8);
}

static inline __device__ __host__ void _TurnBitOff(uint8_t *const bits, std::ptrdiff_t i)
{
    bits[i / 8] &= _FlippedBitmask(i % 8);
}

static inline size_t _CeilToByteLength(size_t n)
{
    return (n + 7) & ~7;
}

static inline size_t _BytesLengthToBitmapLength(size_t n)
{
    return _CeilToByteLength(n) / 8;
}


size_t get_number_of_bytes_for_valid (size_t column_size) {
    return sizeof(gdf_valid_type) * (column_size + GDF_VALID_BITSIZE - 1) / GDF_VALID_BITSIZE;
}

std::string chartobin(gdf_valid_type c, int size = 8)
{
    std::string bin;
    bin.resize(size);
    bin[0] = 0;
    int i;
    for (i = 0; i < size; i++)
    {
        bin[i] = (c % 2) + '0';
        c /= 2;
    }
    return bin;
}

std::string gdf_valid_to_str(gdf_valid_type *valid, size_t column_size)
{
    size_t n_bytes = get_number_of_bytes_for_valid(column_size);
    std::string response;
    for (int i = 0; i < n_bytes; i++)
    {
        int length = n_bytes != i + 1 ? GDF_VALID_BITSIZE : column_size - GDF_VALID_BITSIZE * (n_bytes - 1);
        auto result = chartobin(valid[i], length);
        response += std::string(result);
    }
    return response;
}


auto print_binary(gdf_valid_type n, int size) -> void {
    std::cout << chartobin(n) << "\t sz: " <<  size <<  "\tbinary: " << chartobin(n, size) << std::endl;
}


template <typename ValueType = int8_t>
auto print_column(gdf_column * column) -> void {
    ValueType* host_out = (ValueType*)column->data;
    auto bitmap = column->valid;
    std::cout<<"Printing Column\t null_count:" << column->null_count << "\t type " << column->dtype <<  std::endl;
    int  n_bytes =  sizeof(int8_t) * (column->size + GDF_VALID_BITSIZE - 1) / GDF_VALID_BITSIZE;
     for(int i = 0; i < column->size; i++) {
         int col_position =  i / 8;
         int length_col = n_bytes != col_position+1 ? GDF_VALID_BITSIZE : column->size - GDF_VALID_BITSIZE * (n_bytes - 1);
         int bit_offset =  (length_col - 1) - (i % 8);
         ValueType value    = static_cast<ValueType *>(column->data)[i];
         if ( bitmap[i / 8] & (1 << (i % 8)) ) {
             std::cout << "host_out[" << i << "] = " << value <<"\t\tvalid="<< 1 <<std::endl;
         } else {
             std::cout << "host_out[" << i << "] = " << '\0' <<"\t\tvalid="<< 0 <<std::endl;
         }
     }
    for (int i = 0; i < n_bytes; i++) {
        int length = n_bytes != i+1 ? GDF_VALID_BITSIZE : column->size - GDF_VALID_BITSIZE * (n_bytes - 1);
        print_binary(bitmap[i], length);
    }
//    delete[] host_out;
//    delete[] host_valid_out;
    std::cout<<std::endl<<std::endl;
}


// // #define USING_THRUST_FOR_DEF_LEVELS 1

// static inline size_t _GenerateNullBitmap(const int16_t *const levels, const size_t levels_length, const int16_t max_definition_level, uint8_t *const null_bitmap_ptr)
// {
//     size_t null_count = 0;
//     if (max_definition_level > 0)
//     {

// #ifdef USING_GPU_FOR_DEF_LEVELS
//         thrust::device_vector<int16_t> d_levels(levels, levels + levels_length);

//         thrust::transform(thrust::device,
//                           thrust::counting_iterator<int>(0),
//                           thrust::counting_iterator<int>(levels_length),
//                           d_levels.begin(),
//                           thrust::make_discard_iterator(),
//                           bitmask_functor{max_definition_level, null_bitmap_ptr});
//         @todo : null_count in gpu
// #else
//         auto num_chars = _BytesLengthToBitmapLength(levels_length);
//         uint8_t *h_null_bitmap = new uint8_t[num_chars];
//         for (int i = 0; i < levels_length; ++i)
//         {
//             if (levels[i] == max_definition_level)
//             {
//                 _TurnBitOn(h_null_bitmap, i);
//             }
//             else
//             {
//                 null_count += 1;
//             }
//         }
//         cudaMemcpy(null_bitmap_ptr, h_null_bitmap, num_chars, cudaMemcpyHostToDevice);
// #endif // ! USING_GPU_FOR_DEF_LEVELS
//     }
//     else
//     {
//         auto num_chars = _BytesLengthToBitmapLength(levels_length);
//         std::cout << "num_chars for valid: " << num_chars << std::endl;
//         thrust::fill(thrust::device, null_bitmap_ptr, null_bitmap_ptr + num_chars - 1, 255);
//         uint8_t last_char_value = 0;
//         size_t levels_length_prev = levels_length - levels_length % 8;
//         std::cout << "from to: " << levels_length_prev << "->" << levels_length << std::endl;
//         size_t bit_index = 0;
//         for (int index = levels_length_prev; index < levels_length; ++index)
//         {
//             _TurnBitOn(&last_char_value, bit_index);
//             bit_index++;
//         }
//         thrust::fill(thrust::device, null_bitmap_ptr + num_chars - 1, null_bitmap_ptr + num_chars, last_char_value);
//     }
//     return null_count;
// }

//@todo
// 1. read levels using gpu_decoder
// 2.

struct is_equal
{
    int16_t max_definition_level;

    is_equal(int16_t max_definition_level)
        : max_definition_level(max_definition_level)
    {

    }
    __host__ __device__ bool operator()(const int16_t &x)
    {
        return x == max_definition_level;
    }
};

// expands data vector that does not contain nulls into a representation that has indeterminate values where there should be nulls
// A vector of int work_space needs to be allocated to hold the map for the scatter operation. The workspace should be of size batch_size
template <typename T>
void compact_to_sparse_for_nulls(T *data_in, T *data_out, const int16_t *definition_levels, int16_t max_definition_level,
                                 int64_t batch_size, int *work_space)
{
    is_equal op(max_definition_level);
    auto out_iter = thrust::copy_if(thrust::device,
                                    thrust::counting_iterator<int>(0),
                                    thrust::counting_iterator<int>(batch_size),
                                    definition_levels,
                                    work_space,
                                    op);
    int num_not_null = out_iter - work_space;
    thrust::scatter(thrust::device, data_in, data_in + num_not_null, work_space, data_out);
}



struct bitmask_functor : public thrust::binary_function<int, int16_t, int>
{
    uint8_t *const valid_bits;
    int16_t max_definition_level;
    int64_t valid_bits_offset;

    bitmask_functor(int max_definition_level, uint8_t *const valid_bits, int64_t valid_bits_offset)
        : max_definition_level(max_definition_level),
          valid_bits(valid_bits),
          valid_bits_offset(valid_bits_offset)
    {
    }
    __host__ __device__ int operator()(int i, int16_t level)
    {
        int iter = valid_bits_offset + i;
        if (level == max_definition_level) {
            valid_bits[iter / 8]  |= _ByteWithBit(iter % 8);
        } else if (level < max_definition_level){
            valid_bits[iter / 8]  |= _FlippedBitmask(iter % 8);
        }
        return 0;
    }
};

static inline void
_DefinitionLevelsToBitmap(const std::int16_t *def_levels,
                          std::int64_t        levels_length,
                          const std::int16_t  max_definition_level,
                          std::int64_t *      values_read,
                          std::int64_t *      null_count,
                          std::uint8_t *      null_bitmap_ptr,
                          const std::int64_t  valid_bits_offset) {

    ::arrow::internal::BitmapWriter valid_bits_writer(null_bitmap_ptr, valid_bits_offset, levels_length);

    for (std::int64_t i = 0; i < levels_length; ++i) {
        if (def_levels[i] == max_definition_level) {
            valid_bits_writer.Set();
        } else {
            if (def_levels[i] < max_definition_level) {
                valid_bits_writer.Clear();
                *null_count += 1;
            } else {
                throw ::parquet::ParquetException(
                        "definition level exceeds maximum");
            }
        }

        valid_bits_writer.Next();
    }
    valid_bits_writer.Finish();
    //// *values_read = valid_bits_writer.position();



    //  if (max_definition_level > 0) {
    //     thrust::device_vector<int16_t> d_levels(def_levels, def_levels + levels_length);
    //     thrust::transform(thrust::device,
    //                       thrust::counting_iterator<int>(0),
    //                       thrust::counting_iterator<int>(levels_length),
    //                       d_levels.begin(),
    //                       thrust::make_discard_iterator(),
    //                       bitmask_functor{max_definition_level, null_bitmap_ptr, valid_bits_offset});
    // } else {
    //     std::cout <<  "$$CASE: max_definition_level == 0 " << std::endl;
    //     auto num_chars = _BytesLengthToBitmapLength(levels_length);
    //     thrust::fill(thrust::device, null_bitmap_ptr, null_bitmap_ptr + num_chars - 1, 255);
    //     uint8_t last_char_value = 0;
    //     size_t levels_length_prev = levels_length - levels_length % 8;
    //     size_t bit_index = 0;
    //     for (int index = levels_length_prev; index < levels_length; ++index)
    //     {
    //         _TurnBitOn(&last_char_value, bit_index);
    //         bit_index++;
    //     }
    //     thrust::fill(thrust::device, null_bitmap_ptr + num_chars - 1, null_bitmap_ptr + num_chars, last_char_value);
    // }
}

// static inline void
// _DefinitionLevelsToBitmap(const std::int16_t *def_levels,
//                           std::int64_t        num_def_levels,
//                           const std::int16_t  max_definition_level,
//                           std::int64_t *      values_read,
//                           std::int64_t *      null_count,
//                           std::uint8_t *      d_valid_bits,
//                           const std::int64_t  valid_bits_offset) {

//     auto n_bytes = get_number_of_bytes_for_valid (values_read);
//     std::uint8_t * valid_bits = new std::uint8_t[n_bytes];

//     ::arrow::internal::BitmapWriter valid_bits_writer(valid_bits, valid_bits_offset, num_def_levels);

//     for (std::int64_t i = 0; i < num_def_levels; ++i) {
//         if (def_levels[i] == max_definition_level) {
//             valid_bits_writer.Set();
//         } else {
//             if (def_levels[i] < max_definition_level) {
//                 valid_bits_writer.Clear();
//                 *null_count += 1;
//             } else {
//                 throw ::parquet::ParquetException(
//                         "definition level exceeds maximum");
//             }
//         }

//         valid_bits_writer.Next();
//     }
//     valid_bits_writer.Finish();
//     *values_read = valid_bits_writer.position();

//     cudaMemcpy(d_valid_bits, valid_bits, n_bytes, cudaMemcpyHostToDevice);
//     delete []valid_bits;
// }

template <class DataType>
size_t ColumnReader<DataType>::ToGdfColumn(std::int16_t *const definition_levels, std::int16_t *const repetition_levels,
                                           const gdf_column &column)
{
    using c_type = typename DataType::c_type;

    c_type *const values = static_cast<c_type *const>(column.data);
    std::uint8_t *const valid_bits = static_cast<std::uint8_t *>(column.valid);

    // size_t values_to_read = num_buffered_values_ - num_decoded_values_;
    size_t values_to_read = std::min<size_t>(8, num_buffered_values_ - num_decoded_values_);

    int64_t values_read;
    int64_t rows_read_total = 0;
    int64_t null_count = 0;
    int64_t values_read_counter = 0;

    auto n_bytes = get_number_of_bytes_for_valid (num_buffered_values_);// temporal
    std::uint8_t * h_valid_bits = new std::uint8_t[n_bytes];
    while (this->HasNext()) {
        int64_t rows_read = this->ReadBatch(static_cast<std::int64_t>(values_to_read),
                                            definition_levels  + rows_read_total,
                                            nullptr,
                                            static_cast<T *>(values + values_read_counter), // corregir saltos de values
                                            &values_read);

        _DefinitionLevelsToBitmap(definition_levels  + rows_read_total,
                                  rows_read,
                                  descr_->max_definition_level(),
                                  &values_read,
                                  &null_count,
                                  h_valid_bits, //valid_bits
                                  rows_read_total);

        rows_read_total += rows_read;
        values_read_counter += values_read;
    }
    cudaMemcpy(valid_bits, h_valid_bits, n_bytes, cudaMemcpyHostToDevice);

    if (rows_read_total != values_read_counter) {
        thrust::device_vector<int> work_space_vector(rows_read_total);
        int* work_space = thrust::raw_pointer_cast(work_space_vector.data());
        thrust::device_vector<c_type> d_values_in(values, values + rows_read_total);
        thrust::device_vector<int16_t> d_levels(definition_levels, definition_levels + rows_read_total);

        compact_to_sparse_for_nulls(thrust::raw_pointer_cast(d_values_in.data()),
                                    values,
                                    thrust::raw_pointer_cast(d_levels.data()),
                                    descr_->max_definition_level(),
                                    rows_read_total,
                                    work_space);
    }
    return static_cast<std::size_t>(rows_read_total);
}

#endif

template class ColumnReader<::parquet::BooleanType>;
template class ColumnReader<::parquet::Int32Type>;
template class ColumnReader<::parquet::Int64Type>;
template class ColumnReader<::parquet::FloatType>;
template class ColumnReader<::parquet::DoubleType>;

} // namespace parquet
} // namespace gdf