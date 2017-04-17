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

#include "results.hpp"

#include "impl/realm_coordinator.hpp"
#include "impl/results_notifier.hpp"
#include "object_schema.hpp"
#include "object_store.hpp"
#include "schema.hpp"
#include "util/compiler.hpp"
#include "util/format.hpp"

#include <stdexcept>

using namespace realm;

Results::Results(const Results&) = default;
Results& Results::operator=(const Results&) = default;
Results::Results(Results&&) = default;
Results& Results::operator=(Results&&) = default;

const ObjectSchema& Results::get_object_schema() const
{
    validate_read();

    if (!m_object_schema) {
        REALM_ASSERT(get_realm());
        auto it = get_realm()->schema().find(get_object_type());
        REALM_ASSERT(it != get_realm()->schema().end());
        m_object_schema = &*it;
    }

    return *m_object_schema;
}

StringData Results::get_object_type() const noexcept
{
    if (!table()) {
        return StringData();
    }

    return ObjectStore::object_type_for_table_name(table()->get_name());
}

RowExpr Results::get(size_t row_ndx)
{
    validate_read();
    switch (get_mode()) {
        case Mode::Empty: break;
        case Mode::Table:
            if (row_ndx < table()->size())
                return table()->get(row_ndx);
            break;
        case Mode::LinkView:
            if (update_linkview()) {
                if (row_ndx < get_linkview()->size())
                    return get_linkview()->get(row_ndx);
                break;
            }
            REALM_FALLTHROUGH;
        case Mode::Query:
        case Mode::TableView:
            update_tableview();
            if (row_ndx >= tableview().size())
                break;
            if (!auto_update() && !tableview().is_row_attached(row_ndx))
                return {};
            return tableview().get(row_ndx);
    }

    throw OutOfBoundsIndexException{row_ndx, size()};
}

util::Optional<RowExpr> Results::first()
{
    validate_read();
    switch (get_mode()) {
        case Mode::Empty:
            return none;
        case Mode::Table:
            return table()->size() == 0 ? util::none : util::make_optional(table()->front());
        case Mode::LinkView:
            if (update_linkview())
                return get_linkview()->size() == 0 ? util::none : util::make_optional(get_linkview()->get(0));
            REALM_FALLTHROUGH;
        case Mode::Query:
        case Mode::TableView:
            update_tableview();
            if (tableview().size() == 0)
                return util::none;
            else if (auto_update() && !tableview().is_row_attached(0))
                return RowExpr();
            return tableview().front();
    }
    REALM_UNREACHABLE();
}

util::Optional<RowExpr> Results::last()
{
    validate_read();
    switch (get_mode()) {
        case Mode::Empty:
            return none;
        case Mode::Table:
            return table()->size() == 0 ? util::none : util::make_optional(table()->back());
        case Mode::LinkView:
            if (update_linkview())
                return get_linkview()->size() == 0 ? util::none : util::make_optional(get_linkview()->get(get_linkview()->size() - 1));
            REALM_FALLTHROUGH;
        case Mode::Query:
        case Mode::TableView:
            update_tableview();
            auto s = tableview().size();
            if (s == 0)
                return util::none;
            else if (!auto_update() && !tableview().is_row_attached(s - 1))
                return RowExpr();
            return tableview().back();
    }
    REALM_UNREACHABLE();
}

size_t Results::index_of(Row const& row)
{
    validate_read();
    if (!row) {
        throw DetatchedAccessorException{};
    }
    if (table() && row.get_table() != table()) {
        throw IncorrectTableException(
            ObjectStore::object_type_for_table_name(table()->get_name()),
            ObjectStore::object_type_for_table_name(row.get_table()->get_name()),
            "Attempting to get the index of a Row of the wrong type"
        );
    }
    return index_of(row.get_index());
}

size_t Results::index_of(size_t row_ndx)
{
    validate_read();
    switch (get_mode()) {
        case Mode::Empty:
            return not_found;
        case Mode::Table:
            return row_ndx;
        case Mode::LinkView:
            if (update_linkview())
                return get_linkview()->find(row_ndx);
            REALM_FALLTHROUGH;
        case Mode::Query:
        case Mode::TableView:
            update_tableview();
            return tableview().find_by_source_ndx(row_ndx);
    }
    REALM_UNREACHABLE();
}

template<typename Int, typename Float, typename Double, typename Timestamp>
util::Optional<Mixed> Results::aggregate(size_t column,
                                         const char* name,
                                         Int agg_int, Float agg_float,
                                         Double agg_double, Timestamp agg_timestamp)
{
    validate_read();
    if (!table())
        return none;
    if (column > table()->get_column_count())
        throw OutOfBoundsIndexException{column, table()->get_column_count()};

    auto do_agg = [&](auto const& getter) -> util::Optional<Mixed> {
        switch (get_mode()) {
            case Mode::Empty:
                return none;
            case Mode::Table:
                return util::Optional<Mixed>(getter(*table()));
            case Mode::LinkView:
                switch_to_query();
                REALM_FALLTHROUGH;
            case Mode::Query:
            case Mode::TableView:
                this->update_tableview();
                return util::Optional<Mixed>(getter(tableview()));
        }

        REALM_UNREACHABLE();
    };

    switch (table()->get_column_type(column))
    {
        case type_Timestamp: return do_agg(agg_timestamp);
        case type_Double: return do_agg(agg_double);
        case type_Float: return do_agg(agg_float);
        case type_Int: return do_agg(agg_int);
        default:
            throw UnsupportedColumnTypeException{column, table(), name};
    }
}

util::Optional<Mixed> Results::max(size_t column)
{
    size_t return_ndx = npos;
    auto results = aggregate(column, "max",
                             [&](auto const& table) { return table.maximum_int(column, &return_ndx); },
                             [&](auto const& table) { return table.maximum_float(column, &return_ndx); },
                             [&](auto const& table) { return table.maximum_double(column, &return_ndx); },
                             [&](auto const& table) { return table.maximum_timestamp(column, &return_ndx); });
    return return_ndx == npos ? none : results;
}

util::Optional<Mixed> Results::min(size_t column)
{
    size_t return_ndx = npos;
    auto results = aggregate(column, "min",
                             [&](auto const& table) { return table.minimum_int(column, &return_ndx); },
                             [&](auto const& table) { return table.minimum_float(column, &return_ndx); },
                             [&](auto const& table) { return table.minimum_double(column, &return_ndx); },
                             [&](auto const& table) { return table.minimum_timestamp(column, &return_ndx); });
    return return_ndx == npos ? none : results;
}

util::Optional<Mixed> Results::sum(size_t column)
{
    return aggregate(column, "sum",
                     [=](auto const& table) { return table.sum_int(column); },
                     [=](auto const& table) { return table.sum_float(column); },
                     [=](auto const& table) { return table.sum_double(column); },
                     [=](auto const&) -> util::None { throw UnsupportedColumnTypeException{column, table(), "sum"}; });
}

util::Optional<Mixed> Results::average(size_t column)
{
    // Initial value to make gcc happy
    size_t value_count = 0;
    auto results = aggregate(column, "average",
                             [&](auto const& table) { return table.average_int(column, &value_count); },
                             [&](auto const& table) { return table.average_float(column, &value_count); },
                             [&](auto const& table) { return table.average_double(column, &value_count); },
                             [&](auto const&) -> util::None { throw UnsupportedColumnTypeException{column, table(), "average"}; });
    return value_count == 0 ? none : results;
}

Results Results::sort(realm::SortDescriptor&& sort) const
{
    return Results(get_realm(), get_query(), std::move(sort), get_distinct());
}

Results Results::filter(Query&& q) const
{
    return Results(get_realm(), get_query().and_query(std::move(q)), get_sort(), get_distinct());
}

// FIXME: The current implementation of distinct() breaks the Results API.
// This is tracked by the following issues:
// - https://github.com/realm/realm-object-store/issues/266
// - https://github.com/realm/realm-core/issues/2332
Results Results::distinct(realm::SortDescriptor&& uniqueness)
{
    auto tv = get_tableview();
    tv.distinct(uniqueness);
    return Results(get_realm(), std::move(tv), get_sort(), std::move(uniqueness));
}

Results Results::snapshot() const &
{
    validate_read();
    return Results(*this).snapshot();
}

Results Results::snapshot() &&
{
    ResultsBase::snapshot();
    return std::move(*this);
}
