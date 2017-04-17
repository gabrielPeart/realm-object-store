////////////////////////////////////////////////////////////////////////////
//
// Copyright 2015 Realm Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////

#ifndef REALM_RESULTS_HPP
#define REALM_RESULTS_HPP

#include "impl/results_base.hpp"

namespace realm {
class Mixed;
class ObjectSchema;

class Results : public _impl::ResultsBase {
public:
    // Results can be either be backed by nothing, a thin wrapper around a table,
    // or a wrapper around a query and a sort order which creates and updates
    // the tableview as needed
    Results() = default;
    using ResultsBase::ResultsBase;

    // Results is copyable and moveable
    Results(Results&&);
    Results& operator=(Results&&);
    Results(const Results&);
    Results& operator=(const Results&);

    // Object schema describing the vendored object type
    const ObjectSchema &get_object_schema() const;

    // Get the object type which will be returned by get()
    StringData get_object_type() const noexcept;

    // Get the row accessor for the given index
    // Throws OutOfBoundsIndexException if index >= size()
    RowExpr get(size_t index);

    // Get a row accessor for the first/last row, or none if the results are empty
    // More efficient than calling size()+get()
    util::Optional<RowExpr> first();
    util::Optional<RowExpr> last();

    // Get the first index of the given row in this results, or not_found
    // Throws DetachedAccessorException if row is not attached
    // Throws IncorrectTableException if row belongs to a different table
    size_t index_of(size_t row_ndx);
    size_t index_of(Row const& row);

    // Create a new Results by further filtering or sorting this Results
    Results filter(Query&& q) const;
    Results sort(SortDescriptor&& sort) const;

    // Create a new Results by removing duplicates
    // FIXME: The current implementation of distinct() breaks the Results API.
    // This is tracked by the following issues:
    // - https://github.com/realm/realm-object-store/issues/266
    // - https://github.com/realm/realm-core/issues/2332
    Results distinct(SortDescriptor&& uniqueness);
    
    // Return a snapshot of this Results that never updates to reflect changes in the underlying data.
    Results snapshot() const &;
    Results snapshot() &&;

    // Get the min/max/average/sum of the given column
    // All but sum() returns none when there are zero matching rows
    // sum() returns 0, except for when it returns none
    // Throws UnsupportedColumnTypeException for sum/average on timestamp or non-numeric column
    // Throws OutOfBoundsIndexException for an out-of-bounds column
    util::Optional<Mixed> max(size_t column);
    util::Optional<Mixed> min(size_t column);
    util::Optional<Mixed> average(size_t column);
    util::Optional<Mixed> sum(size_t column);

private:
    mutable const ObjectSchema *m_object_schema = nullptr;
    template<typename Int, typename Float, typename Double, typename Timestamp>
    util::Optional<Mixed> aggregate(size_t column,
                                    const char* name,
                                    Int agg_int, Float agg_float,
                                    Double agg_double, Timestamp agg_timestamp);
};
}

#endif /* REALM_RESULTS_HPP */
