#include "binary/binary2/type.h"

namespace gdf {

    const char* getTypeName(gdf_dtype type) {
        switch (type) {
            case GDF_INT8:
                return "int8_t";
            case GDF_INT16:
                return "int16_t";
            case GDF_INT32:
            case GDF_DATE32:
                return "int32_t";
            case GDF_INT64:
            case GDF_DATE64:
            case GDF_TIMESTAMP:
                return "int64_t";
            case GDF_UINT8:
                return "uint8_t";
            case GDF_UINT16:
                return "uint16_t";
            case GDF_UINT32:
                return "uint32_t";
            case GDF_UINT64:
                return "uint64_t";
            case GDF_FLOAT32:
                return "float";
            case GDF_FLOAT64:
                return "double";
            default:
                return "double";
        }
    }

    const char* getOperatorName(gdf_binary_operator ope) {
        switch (ope) {
            case GDF_ADD:
                return "Add";
            case GDF_SUB:
                return "Sub";
            case GDF_MUL:
                return "Mul";
            case GDF_DIV:
                return "Div";
            case GDF_TRUE_DIV:
                return "TrueDiv";
            case GDF_FLOOR_DIV:
                return "FloorDiv";
            case GDF_MOD:
                return "Pow";
            //GDF_COMBINE,
            case GDF_COMBINE_FIRST:
                return "CombineFirst";
            case GDF_ROUND:
                return "Round";
            case GDF_EQUAL:
                return "Equal";
            case GDF_NOT_EQUAL:
                return "NotEqual";
            case GDF_LESS:
                return "Less";
            case GDF_GREATER:
                return "Greater";
            case GDF_LESS_EQUAL:
                return "LessEqual";
            case GDF_GREATER_EQUAL:
                return "GreaterEqual";
            //GDF_PRODUCT,
            //GDF_DOT
            default:
                return "None";
        }
    }

}