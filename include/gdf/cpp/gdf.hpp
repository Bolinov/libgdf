/*
 * Copyright (c) 2018, BlazingDB Inc.
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

/**
 *
 * @return
 */
#ifndef GDF_GDF_HPP_
#define GDF_GDF_HPP_

#include <cstdlib>
#include "types.h"
// #include "io_types.h"
// #include "convert_types.h"

// TODO: Move this elsewhere
template <typename T>
size_t size_in_bits() { return sizeof(T) * CHAR_BITS; }

#include "operators.hpp"
#include "types.hpp"

#endif /* GDF_GDF_HPP_ */
