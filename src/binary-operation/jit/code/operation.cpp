/*
 * Copyright 2018-2019 BlazingDB, Inc.
 *     Copyright 2018 Christian Noboa Mardini <christian@blazingdb.com>
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

namespace gdf {
namespace binops {
namespace jit {
namespace code {

const char* operation =
R"***(
#pragma once

    struct Add {
        template <typename TypeOut, typename TypeVax, typename TypeVay>
        static TypeOut operate(TypeVax x, TypeVay y) {
            using Common = CommonNumber<TypeVax, TypeVay>;
            return (TypeOut)((Common)x + (Common)y);
        }
    };

    struct Sub {
        template <typename TypeOut, typename TypeVax, typename TypeVay>
        static TypeOut operate(TypeVax x, TypeVay y) {
            using Common = CommonNumber<TypeVax, TypeVay>;
            return (TypeOut)((Common)x - (Common)y);
        }
    };

    struct Mul {
        template <typename TypeOut, typename TypeVax, typename TypeVay>
        static TypeOut operate(TypeVax x, TypeVay y) {
            using Common = CommonNumber<TypeVax, TypeVay>;
            return (TypeOut)((Common)x * (Common)y);
        }
    };

    struct Div {
        template <typename TypeOut, typename TypeVax, typename TypeVay>
        static TypeOut operate(TypeVax x, TypeVay y) {
            using Common = CommonNumber<TypeVax, TypeVay>;
            return (TypeOut)((Common)x / (Common)y);
        }
    };

    struct TrueDiv {
        template <typename TypeOut, typename TypeVax, typename TypeVay>
        static TypeOut operate(TypeVax x, TypeVay y) {
            return (TypeOut)((double)x / (double)y);
        }
    };

    struct FloorDiv {
        template <typename TypeOut, typename TypeVax, typename TypeVay>
        static TypeOut operate(TypeVax x, TypeVay y) {
            return (TypeOut)floor((double)x / (double)y);
        }
    };

    struct Mod {
        template <typename TypeOut,
                  typename TypeVax,
                  typename TypeVay,
                  typename Common = CommonNumber<TypeVax, TypeVay>,
                  enableIf<(isIntegral<Common>)>* = nullptr>
        static TypeOut operate(TypeVax x, TypeVay y) {
            return (TypeOut)((Common)x % (Common)y);
        }

        template <typename TypeOut,
                  typename TypeVax,
                  typename TypeVay,
                  typename Common = CommonNumber<TypeVax, TypeVay>,
                  enableIf<(isFloat<Common>)>* = nullptr>
        static TypeOut operate(TypeVax x, TypeVay y) {
            return (TypeOut)fmodf((Common)x, (Common)y);
        }

        template <typename TypeOut,
                  typename TypeVax,
                  typename TypeVay,
                  typename Common = CommonNumber<TypeVax, TypeVay>,
                  enableIf<(isDouble<Common>)>* = nullptr>
        static TypeOut operate(TypeVax x, TypeVay y) {
            return (TypeOut)fmod((Common)x, (Common)y);
        }
    };

    struct Pow {
        template <typename TypeOut, typename TypeVax, typename TypeVay>
        static TypeOut operate(TypeVax x, TypeVay y) {
            return (TypeOut)pow((double)x, (double)y);
        }
    };

    struct Equal {
        template <typename TypeOut, typename TypeVax, typename TypeVay>
        static TypeOut operate(TypeVax x, TypeVay y) {
            using Common = CommonNumber<TypeVax, TypeVay>;
            return (TypeOut)((Common)x == (Common)y);
        }
    };

    struct NotEqual {
        template <typename TypeOut, typename TypeVax, typename TypeVay>
        static TypeOut operate(TypeVax x, TypeVay y) {
            using Common = CommonNumber<TypeVax, TypeVay>;
            return (TypeOut)((Common)x != (Common)y);
        }
    };

    struct Less {
        template <typename TypeOut, typename TypeVax, typename TypeVay>
        static TypeOut operate(TypeVax x, TypeVay y) {
            using Common = CommonNumber<TypeVax, TypeVay>;
            return (TypeOut)((Common)x < (Common)y);
        }
    };

    struct Greater {
        template <typename TypeOut, typename TypeVax, typename TypeVay>
        static TypeOut operate(TypeVax x, TypeVay y) {
            using Common = CommonNumber<TypeVax, TypeVay>;
            return (TypeOut)((Common)x > (Common)y);
        }
    };

    struct LessEqual {
        template <typename TypeOut, typename TypeVax, typename TypeVay>
        static TypeOut operate(TypeVax x, TypeVay y) {
            using Common = CommonNumber<TypeVax, TypeVay>;
            return (TypeOut)((Common)x <= (Common)y);
        }
    };

    struct GreaterEqual {
        template <typename TypeOut, typename TypeVax, typename TypeVay>
        static TypeOut operate(TypeVax x, TypeVay y) {
            using Common = CommonNumber<TypeVax, TypeVay>;
            return (TypeOut)((Common)x >= (Common)y);
        }
    };

)***";

/*
 * The following code could be used to detect overflow or underflow
 * using 'Bit Hacks' in the operations, that's why the operation is
 * divided into signed, unsigned and double functions. It's required
 * to create a new field on gdf_column for this feature.
 *
 *     struct Add {
 *      template <typename TypeOut,
 *                typename TypeVax,
 *                typename TypeVay,
 *                typename Common = CommonNumber<TypeVax, TypeVay>,
 *                enableIf<(isIntegralSigned<Common>)>* = nullptr>
 *      __device__
 *      TypeOut operate(TypeVax x, TypeVay y) {
 *          return (TypeOut)((Common)x + (Common)y);
 *      }
 *
 *      template <typename TypeOut,
 *                typename TypeVax,
 *                typename TypeVay,
 *                typename Common = CommonNumber<TypeVax, TypeVay>,
 *                enableIf<(isIntegralUnsigned<Common>)>* = nullptr>
 *      __device__
 *      TypeOut operate(TypeVax x, TypeVay y) {
 *          return (TypeOut)((Common)x + (Common)y);
 *      }
 *
 *      template <typename TypeOut,
 *                typename TypeVax,
 *                typename TypeVay,
 *                typename Common = CommonNumber<TypeVax, TypeVay>,
 *                enableIf<(isFloatingPoint<Common>)>* = nullptr>
 *      __device__
 *      TypeOut operate(TypeVax x, TypeVay y) {
 *          return (TypeOut)((Common)x + (Common)y);
 *      }
 *  };
 */

} // namespace code
} // namespace jit
} // namespace binops
} // namespace gdf
