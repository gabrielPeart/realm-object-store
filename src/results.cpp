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

Results::Results() = default;
Results::~Results() = default;

Results::Results(SharedRealm r, Query q, SortDescriptor s, SortDescriptor d)
: m_realm(std::move(r))
, m_query(std::move(q))
, m_table(m_query.get_table())
, m_sort(std::move(s))
, m_distinct(std::move(d))
, m_mode(Mode::Query)
{
}

Results::Results(SharedRealm r, Table& table)
: m_realm(std::move(r))
, m_mode(Mode::Table)
{
    m_table.reset(&table);
}

Results::Results(SharedRealm r, LinkViewRef lv, util::Optional<Query> q, SortDescriptor s)
: m_realm(std::move(r))
, m_link_view(lv)
, m_sort(std::move(s))
, m_mode(Mode::LinkView)
{
    m_table.reset(&lv->get_target_table());
    if (q) {
        m_query = std::move(*q);
        m_mode = Mode::Query;
    }
}

Results::Results(SharedRealm r, TableView tv, SortDescriptor s, SortDescriptor d)
: m_realm(std::move(r))
, m_table_view(std::move(tv))
, m_sort(std::move(s))
, m_distinct(std::move(d))
, m_mode(Mode::TableView)
{
    m_table.reset(&m_table_view.get_parent());
}

Results::Results(const Results&) = default;
Results& Results::operator=(const Results&) = default;

Results::Results(Results&& other)
: m_realm(std::move(other.m_realm))
, m_object_schema(std::move(other.m_object_schema))
, m_query(std::move(other.m_query))
, m_table_view(std::move(other.m_table_view))
, m_link_view(std::move(other.m_link_view))
, m_table(std::move(other.m_table))
, m_sort(std::move(other.m_sort))
, m_distinct(std::move(other.m_distinct))
, m_notifier(std::move(other.m_notifier))
, m_mode(other.m_mode)
, m_update_policy(other.m_update_policy)
, m_has_used_table_view(other.m_has_used_table_view)
, m_wants_background_updates(other.m_wants_background_updates)
{
    if (m_notifier) {
        m_notifier->target_results_moved(other, *this);
    }
}

Results& Results::operator=(Results&& other)
{
    this->~Results();
    new (this) Results(std::move(other));
    return *this;
}

bool Results::is_valid() const
{
    if (m_realm)
        m_realm->verify_thread();

    if (m_table && !m_table->is_attached())
        return false;

    return true;
}

void Results::validate_read() const
{
    // is_valid ensures that we're on the correct thread.
    if (!is_valid())
        throw InvalidatedException();
}

void Results::validate_write() const
{
    validate_read();
    if (!m_realm || !m_realm->is_in_transaction())
        throw InvalidTransactionException("Must be in a write transaction");
}

size_t Results::size()
{
    validate_read();
    switch (m_mode) {
        case Mode::Empty:    return 0;
        case Mode::Table:    return m_table->size();
        case Mode::LinkView: return m_link_view->size();
        case Mode::Query:
            m_query.sync_view_if_needed();
            if (!m_distinct)
                return m_query.count();
            REALM_FALLTHROUGH;
        case Mode::TableView:
            update_tableview();
            return m_table_view.size();
    }
    REALM_UNREACHABLE();
}

const ObjectSchema& Results::get_object_schema() const
{
    validate_read();

    if (!m_object_schema) {
        REALM_ASSERT(m_realm);
        auto it = m_realm->schema().find(get_object_type());
        REALM_ASSERT(it != m_realm->schema().end());
        m_object_schema = &*it;
    }

    return *m_object_schema;
}


StringData Results::get_object_type() const noexcept
{
    if (!m_table) {
        return StringData();
    }

    return ObjectStore::object_type_for_table_name(m_table->get_name());
}

template<typename T>
constexpr auto is_dereferencable(int) -> decltype(*T(), bool()) { return true; }
template<typename T>
constexpr bool is_dereferencable(...) { return false; }

template<typename T, typename = std::enable_if_t<!is_dereferencable<T>(0)>>
T unbox(T value) { return value; }
template<typename T>
T unbox(util::Optional<T> value) { return *value; }

template<typename T, typename = std::enable_if_t<!is_dereferencable<T>(0)>>
auto get(Table& table, size_t row)
{
    return table.get<T>(0, row);
}

template<typename T, typename = void, typename = std::enable_if_t<is_dereferencable<T>(0)>>
T get(Table& table, size_t row)
{
    if (table.is_null(0, row))
        return util::none;
    return table.get<decltype(unbox(T()))>(0, row);
}

template<>
auto get<RowExpr, void>(Table& table, size_t row)
{
    return table.get(row);
}

template<typename T>
T Results::do_get(size_t row_ndx)
{
    switch (m_mode) {
        case Mode::Query:
        case Mode::Empty:
            REALM_COMPILER_HINT_UNREACHABLE();
        case Mode::Table:
            return ::get<T>(*m_table, row_ndx);
        case Mode::LinkView:
            return ::get<T>(*m_table, m_link_view->get(row_ndx).get_index());
        case Mode::TableView:
            if (m_update_policy == UpdatePolicy::Never && !m_table_view.is_row_attached(row_ndx))
                return {};
            return ::get<T>(*m_table, m_table_view.get(row_ndx).get_index());
    }
}


template<typename T>
T Results::get(size_t row_ndx)
{
    validate_read();
    switch (m_mode) {
        case Mode::Empty: break;
        case Mode::Table:
            if (row_ndx < m_table->size())
                return do_get<T>(row_ndx);
            break;
        case Mode::LinkView:
            if (update_linkview()) {
                if (row_ndx < m_link_view->size())
                    return do_get<T>(row_ndx);
                break;
            }
            REALM_FALLTHROUGH;
        case Mode::Query:
        case Mode::TableView:
            update_tableview();
            if (row_ndx >= m_table_view.size())
                break;
            return do_get<T>(row_ndx);
    }

    throw OutOfBoundsIndexException{row_ndx, size()};
}

template<typename T>
util::Optional<T> Results::first()
{
    validate_read();
    switch (m_mode) {
        case Mode::Empty:
            return none;
        case Mode::Table:
            return m_table->size() == 0 ? util::none : util::make_optional(do_get<T>(0));
        case Mode::LinkView:
            if (update_linkview())
                return m_link_view->size() == 0 ? util::none : util::make_optional(do_get<T>(0));
            REALM_FALLTHROUGH;
        case Mode::Query:
        case Mode::TableView:
            update_tableview();
            if (m_table_view.size() == 0)
                return util::none;
            return do_get<T>(0);
    }
    REALM_UNREACHABLE();
}

template<typename T>
util::Optional<T> Results::last()
{
    validate_read();
    switch (m_mode) {
        case Mode::Empty:
            return none;
        case Mode::Table:
            return m_table->size() == 0 ? util::none : util::make_optional(do_get<T>(size() - 1));
        case Mode::LinkView:
            if (update_linkview())
                return m_link_view->size() == 0 ? util::none : util::make_optional(do_get<T>(size() - 1));
            REALM_FALLTHROUGH;
        case Mode::Query:
        case Mode::TableView:
            update_tableview();
            if (m_table_view.size() == 0)
                return util::none;
            return do_get<T>(m_table_view.size() - 1);
    }
    REALM_UNREACHABLE();
}

bool Results::update_linkview()
{
    REALM_ASSERT(m_update_policy == UpdatePolicy::Auto);

    if (m_sort || m_distinct) {
        m_query = get_query();
        m_mode = Mode::Query;
        update_tableview();
        return false;
    }
    return true;
}

void Results::update_tableview(bool wants_notifications)
{
    if (m_update_policy == UpdatePolicy::Never) {
        REALM_ASSERT(m_mode == Mode::TableView);
        return;
    }

    switch (m_mode) {
        case Mode::Empty:
        case Mode::Table:
        case Mode::LinkView:
            return;
        case Mode::Query:
            m_query.sync_view_if_needed();
            m_table_view = m_query.find_all();
            if (m_sort) {
                m_table_view.sort(m_sort);
            }
            if (m_distinct) {
                m_table_view.distinct(m_distinct);
            }
            m_mode = Mode::TableView;
            REALM_FALLTHROUGH;
        case Mode::TableView:
            if (wants_notifications && !m_notifier && !m_realm->is_in_transaction() && m_realm->can_deliver_notifications()) {
                m_notifier = std::make_shared<_impl::ResultsNotifier>(*this);
                _impl::RealmCoordinator::register_notifier(m_notifier);
            }
            m_has_used_table_view = true;
            m_table_view.sync_if_needed();
            break;
    }
}

template<>
size_t Results::index_of(size_t const& row_ndx)
{
    validate_read();
    switch (m_mode) {
        case Mode::Empty:
            return not_found;
        case Mode::Table:
            return row_ndx;
        case Mode::LinkView:
            if (update_linkview())
                return m_link_view->find(row_ndx);
            REALM_FALLTHROUGH;
        case Mode::Query:
        case Mode::TableView:
            update_tableview();
            return m_table_view.find_by_source_ndx(row_ndx);
    }
    REALM_UNREACHABLE();
}

template<>
size_t Results::index_of(Row const& row)
{
    validate_read();
    if (!row) {
        throw DetatchedAccessorException{};
    }
    if (m_table && row.get_table() != m_table) {
        throw IncorrectTableException(
            ObjectStore::object_type_for_table_name(m_table->get_name()),
            ObjectStore::object_type_for_table_name(row.get_table()->get_name()),
            "Attempting to get the index of a Row of the wrong type"
        );
    }
    return index_of(row.get_index());
}

template<typename T, typename = std::enable_if_t<!is_dereferencable<T>(0)>>
bool is_null(T) { return false; }
template<typename T>
bool is_null(util::Optional<T> value) { return !value; }

template<typename T> size_t find_first(Table& table, T value)
{
    return is_null(value) ? table.find_first_null(0) : table.find_first(0, unbox(value));
}
template<typename T> size_t find_first(TableView& table, T value)
{
    return is_null(value) ? table.get_parent().where(&table).equal(0, realm::null()).find()
                          : table.find_first(0, unbox(value));
}
template<> size_t find_first(TableView& table, bool value)
{
    return table.find_first<int64_t>(0, value);
}
template<> size_t find_first(TableView& table, util::Optional<bool> value)
{
    return is_null(value) ? table.get_parent().where(&table).equal(0, realm::null()).find()
                          : table.find_first<int64_t>(0, unbox(value));
}

#define REALM_FIND_FIRST(Type, type_name) \
    template<> size_t find_first<Type>(Table& table, Type value) \
    { \
        return table.find_first_##type_name(0, value); \
    } \
    template<> size_t find_first<Type>(TableView& table, Type value) \
    { \
        return table.find_first_##type_name(0, value); \
    }
REALM_FIND_FIRST(int64_t, int)
REALM_FIND_FIRST(float, float)
REALM_FIND_FIRST(double, double)
REALM_FIND_FIRST(Timestamp, timestamp)
REALM_FIND_FIRST(StringData, string)
REALM_FIND_FIRST(BinaryData, binary)
#undef REALM_FIND_FIRST

template<typename T>
size_t Results::index_of(T const& value)
{
    validate_read();
    switch (m_mode) {
        default: REALM_COMPILER_HINT_UNREACHABLE();
        case Mode::Empty:
            return not_found;
        case Mode::Table:
            return ::find_first(*m_table, value);
        case Mode::Query:
        case Mode::TableView:
            update_tableview();
            return ::find_first(m_table_view, value);
    }
}

#define REALM_DECLARE_AGGREGATE_FUNCTIONS(name) \
    template<typename T, typename U> util::Optional<T> name(Table&) { throw "unsupported"; } \
    template<typename T, typename U> util::Optional<T> name(TableView&) { throw "unsupported"; }
REALM_DECLARE_AGGREGATE_FUNCTIONS(maximum)
REALM_DECLARE_AGGREGATE_FUNCTIONS(minimum)
REALM_DECLARE_AGGREGATE_FUNCTIONS(average)
REALM_DECLARE_AGGREGATE_FUNCTIONS(sum)
#undef REALM_DECLARE_AGGREGATE_FUNCTIONS

#define REALM_MINMAX_FUNCTION(TableType, name, Type, type_name) \
template<> \
util::Optional<Type> name<Type, Type>(TableType& table) \
{ \
    size_t ndx = 0; \
    auto value = table.name##_##type_name(0, &ndx); \
    return ndx == npos ? none : util::make_optional(value); \
}

REALM_MINMAX_FUNCTION(Table, maximum, int64_t, int)
REALM_MINMAX_FUNCTION(TableView, maximum, int64_t, int)
REALM_MINMAX_FUNCTION(Table, maximum, float, float)
REALM_MINMAX_FUNCTION(TableView, maximum, float, float)
REALM_MINMAX_FUNCTION(Table, maximum, double, double)
REALM_MINMAX_FUNCTION(TableView, maximum, double, double)
REALM_MINMAX_FUNCTION(Table, maximum, Timestamp, timestamp)
REALM_MINMAX_FUNCTION(TableView, maximum, Timestamp, timestamp)

REALM_MINMAX_FUNCTION(Table, minimum, int64_t, int)
REALM_MINMAX_FUNCTION(TableView, minimum, int64_t, int)
REALM_MINMAX_FUNCTION(Table, minimum, float, float)
REALM_MINMAX_FUNCTION(TableView, minimum, float, float)
REALM_MINMAX_FUNCTION(Table, minimum, double, double)
REALM_MINMAX_FUNCTION(TableView, minimum, double, double)
REALM_MINMAX_FUNCTION(Table, minimum, Timestamp, timestamp)
REALM_MINMAX_FUNCTION(TableView, minimum, Timestamp, timestamp)

#undef REALM_MINMAX_FUNCTION

#define REALM_SUM_FUNCTION(TableType, Type, type_name) \
template<> \
util::Optional<Type> sum<Type, Type>(TableType& table) \
{ \
    return table.sum_##type_name(0); \
}

REALM_SUM_FUNCTION(Table, int64_t, int)
REALM_SUM_FUNCTION(TableView, int64_t, int)
REALM_SUM_FUNCTION(Table, float, float)
REALM_SUM_FUNCTION(TableView, double, double)

#undef REALM_SUM_FUNCTION

#define REALM_AVERAGE_FUNCTION(TableType, Type, type_name) \
template<> \
util::Optional<double> average<double, Type>(TableType& table) \
{ \
    size_t non_nil_count = 0; \
    auto value = table.average_##type_name(0, &non_nil_count); \
    return non_nil_count == 0 ? none : make_optional(value); \
}

REALM_AVERAGE_FUNCTION(Table, int64_t, int)
REALM_AVERAGE_FUNCTION(TableView, int64_t, int)
REALM_AVERAGE_FUNCTION(Table, float, float)
REALM_AVERAGE_FUNCTION(TableView, double, double)

#undef REALM_AVERAGE_FUNCTION


template<typename T>
util::Optional<T> Results::max(size_t)
{
    switch (get_mode()) {
        default: REALM_COMPILER_HINT_UNREACHABLE();
        case Mode::Empty:
            return none;
        case Mode::Table:
            return maximum<T, T>(*m_table);
        case Mode::Query:
        case Mode::TableView:
            this->update_tableview();
            return maximum<T, T>(m_table_view);
    }
}

template<typename T>
util::Optional<T> Results::min(size_t)
{
    switch (get_mode()) {
        default: REALM_COMPILER_HINT_UNREACHABLE();
        case Mode::Empty:
            return none;
        case Mode::Table:
            return minimum<T, T>(*m_table);
        case Mode::Query:
        case Mode::TableView:
            this->update_tableview();
            return minimum<T, T>(m_table_view);
    }
}

template<typename T>
util::Optional<T> Results::sum(size_t)
{
    switch (get_mode()) {
        default: REALM_COMPILER_HINT_UNREACHABLE();
        case Mode::Empty:
            return none;
        case Mode::Table:
            return ::sum<T, T>(*m_table);
        case Mode::Query:
        case Mode::TableView:
            this->update_tableview();
            return ::sum<T, T>(m_table_view);
    }
}

template<typename T>
util::Optional<T> Results::average(size_t)
{
    switch (get_mode()) {
        default: REALM_COMPILER_HINT_UNREACHABLE();
        case Mode::Empty:
            return none;
        case Mode::Table:
//            return ::average<double, T>(*m_table);
            return none;
        case Mode::Query:
        case Mode::TableView:
            this->update_tableview();
            return none;
//            return ::average<double, T>(m_table_view);
    }
}

template<typename Int, typename Float, typename Double, typename Timestamp>
util::Optional<Mixed> Results::aggregate(size_t column,
                                         const char* name,
                                         Int agg_int, Float agg_float,
                                         Double agg_double, Timestamp agg_timestamp)
{
    validate_read();
    if (!m_table)
        return none;
    if (column > m_table->get_column_count())
        throw OutOfBoundsIndexException{column, m_table->get_column_count()};

    auto do_agg = [&](auto const& getter) -> util::Optional<Mixed> {
        switch (m_mode) {
            case Mode::Empty:
                return none;
            case Mode::Table:
                return util::Optional<Mixed>(getter(*m_table));
            case Mode::LinkView:
                m_query = this->get_query();
                m_mode = Mode::Query;
                REALM_FALLTHROUGH;
            case Mode::Query:
            case Mode::TableView:
                this->update_tableview();
                return util::Optional<Mixed>(getter(m_table_view));
        }

        REALM_UNREACHABLE();
    };

    switch (m_table->get_column_type(column))
    {
        case type_Timestamp: return do_agg(agg_timestamp);
        case type_Double: return do_agg(agg_double);
        case type_Float: return do_agg(agg_float);
        case type_Int: return do_agg(agg_int);
        default:
            throw UnsupportedColumnTypeException{column, m_table.get(), name};
    }
}

template<>
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

template<>
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

template<>
util::Optional<Mixed> Results::sum(size_t column)
{
    return aggregate(column, "sum",
                     [=](auto const& table) { return table.sum_int(column); },
                     [=](auto const& table) { return table.sum_float(column); },
                     [=](auto const& table) { return table.sum_double(column); },
                     [=](auto const&) -> util::None { throw UnsupportedColumnTypeException{column, m_table.get(), "sum"}; });
}

template<>
util::Optional<Mixed> Results::average(size_t column)
{
    // Initial value to make gcc happy
    size_t value_count = 0;
    auto results = aggregate(column, "average",
                             [&](auto const& table) { return table.average_int(column, &value_count); },
                             [&](auto const& table) { return table.average_float(column, &value_count); },
                             [&](auto const& table) { return table.average_double(column, &value_count); },
                             [&](auto const&) -> util::None { throw UnsupportedColumnTypeException{column, m_table.get(), "average"}; });
    return value_count == 0 ? none : results;
}

void Results::clear()
{
    switch (m_mode) {
        case Mode::Empty:
            return;
        case Mode::Table:
            validate_write();
            m_table->clear();
            break;
        case Mode::Query:
            // Not using Query:remove() because building the tableview and
            // clearing it is actually significantly faster
        case Mode::TableView:
            validate_write();
            update_tableview();

            switch (m_update_policy) {
                case UpdatePolicy::Auto:
                    m_table_view.clear(RemoveMode::unordered);
                    break;
                case UpdatePolicy::Never: {
                    // Copy the TableView because a frozen Results shouldn't let its size() change.
                    TableView copy(m_table_view);
                    copy.clear(RemoveMode::unordered);
                    break;
                }
            }
            break;
        case Mode::LinkView:
            validate_write();
            m_link_view->remove_all_target_rows();
            break;
    }
}

PropertyType Results::get_type() const
{
    validate_read();
    switch (m_mode) {
        case Mode::Empty:
            // FIXME: ?
            return PropertyType::Int;
        case Mode::LinkView:
            return PropertyType::Object;
        case Mode::Query:
        case Mode::TableView:
        case Mode::Table:
            if (m_table->get_index_in_group() != realm::npos)
                return PropertyType::Object;
            return ObjectSchema::from_core_type(*m_table->get_descriptor(), 0);
    }
}

bool Results::is_optional() const noexcept
{
    return m_table && m_table->is_attached() && m_table->is_nullable(0);
}

Query Results::get_query() const
{
    validate_read();
    switch (m_mode) {
        case Mode::Empty:
        case Mode::Query:
            return m_query;
        case Mode::TableView: {
            // A TableView has an associated Query if it was produced by Query::find_all. This is indicated
            // by TableView::get_query returning a Query with a non-null table.
            Query query = m_table_view.get_query();
            if (query.get_table()) {
                return query;
            }

            // The TableView has no associated query so create one with no conditions that is restricted
            // to the rows in the TableView.
            if (m_update_policy == UpdatePolicy::Auto) {
                m_table_view.sync_if_needed();
            }
            return Query(*m_table, std::unique_ptr<TableViewBase>(new TableView(m_table_view)));
        }
        case Mode::LinkView:
            return m_table->where(m_link_view);
        case Mode::Table:
            return m_table->where();
    }
    REALM_UNREACHABLE();
}

TableView Results::get_tableview()
{
    validate_read();
    switch (m_mode) {
        case Mode::Empty:
            return {};
        case Mode::LinkView:
            if (update_linkview())
                return m_table->where(m_link_view).find_all();
            REALM_FALLTHROUGH;
        case Mode::Query:
        case Mode::TableView:
            update_tableview();
            return m_table_view;
        case Mode::Table:
            return m_table->where().find_all();
    }
    REALM_UNREACHABLE();
}

Results Results::sort(realm::SortDescriptor&& sort) const
{
    return Results(m_realm, get_query(), std::move(sort), m_distinct);
}

Results Results::filter(Query&& q) const
{
    return Results(m_realm, get_query().and_query(std::move(q)), m_sort, m_distinct);
}


// FIXME: The current implementation of distinct() breaks the Results API.
// This is tracked by the following issues:
// - https://github.com/realm/realm-object-store/issues/266
// - https://github.com/realm/realm-core/issues/2332
Results Results::distinct(realm::SortDescriptor&& uniqueness)
{
    auto tv = get_tableview();
    tv.distinct(uniqueness);
    return Results(m_realm, std::move(tv), m_sort, std::move(uniqueness));
}

Results Results::snapshot() const &
{
    validate_read();

    return Results(*this).snapshot();
}

Results Results::snapshot() &&
{
    validate_read();

    switch (m_mode) {
        case Mode::Empty:
            return Results();

        case Mode::Table:
        case Mode::LinkView:
            m_query = get_query();
            m_mode = Mode::Query;

            REALM_FALLTHROUGH;
        case Mode::Query:
        case Mode::TableView:
            update_tableview(false);
            m_notifier.reset();
            m_update_policy = UpdatePolicy::Never;
            return std::move(*this);
    }
    REALM_UNREACHABLE();
}

void Results::prepare_async()
{
    if (m_notifier) {
        return;
    }
    if (m_realm->config().read_only()) {
        throw InvalidTransactionException("Cannot create asynchronous query for read-only Realms");
    }
    if (m_realm->is_in_transaction()) {
        throw InvalidTransactionException("Cannot create asynchronous query while in a write transaction");
    }
    if (m_update_policy == UpdatePolicy::Never) {
        throw std::logic_error("Cannot create asynchronous query for snapshotted Results.");
    }

    m_wants_background_updates = true;
    m_notifier = std::make_shared<_impl::ResultsNotifier>(*this);
    _impl::RealmCoordinator::register_notifier(m_notifier);
}

NotificationToken Results::async(std::function<void (std::exception_ptr)> target)
{
    prepare_async();
    auto wrap = [=](CollectionChangeSet, std::exception_ptr e) { target(e); };
    return {m_notifier, m_notifier->add_callback(wrap)};
}

NotificationToken Results::add_notification_callback(CollectionChangeCallback cb) &
{
    prepare_async();
    return {m_notifier, m_notifier->add_callback(std::move(cb))};
}

bool Results::is_in_table_order() const
{
    switch (m_mode) {
        case Mode::Empty:
        case Mode::Table:
            return true;
        case Mode::LinkView:
            return false;
        case Mode::Query:
            return m_query.produces_results_in_table_order() && !m_sort;
        case Mode::TableView:
            return m_table_view.is_in_table_order();
    }
    REALM_UNREACHABLE(); // keep gcc happy
}

void Results::Internal::set_table_view(Results& results, realm::TableView &&tv)
{
    REALM_ASSERT(results.m_update_policy != UpdatePolicy::Never);
    // If the previous TableView was never actually used, then stop generating
    // new ones until the user actually uses the Results object again
    if (results.m_mode == Mode::TableView) {
        results.m_wants_background_updates = results.m_has_used_table_view;
    }

    results.m_table_view = std::move(tv);
    results.m_mode = Mode::TableView;
    results.m_has_used_table_view = false;
    REALM_ASSERT(results.m_table_view.is_in_sync());
    REALM_ASSERT(results.m_table_view.is_attached());
}

namespace realm {
#define REALM_RESULTS_TYPE(T) \
    template T Results::get<T>(size_t); \
    template util::Optional<T> Results::first<T>(); \
    template util::Optional<T> Results::last<T>(); \
    template size_t Results::index_of<T>(T const&); \
    template util::Optional<T> Results::max<T>(size_t); \
    template util::Optional<T> Results::min<T>(size_t); \
    template util::Optional<T> Results::average<T>(size_t); \
    template util::Optional<T> Results::sum<T>(size_t);

template RowExpr Results::get<RowExpr>(size_t);
template util::Optional<RowExpr> Results::first<RowExpr>();
template util::Optional<RowExpr> Results::last<RowExpr>();

REALM_RESULTS_TYPE(bool)
REALM_RESULTS_TYPE(int64_t)
REALM_RESULTS_TYPE(float)
REALM_RESULTS_TYPE(double)
REALM_RESULTS_TYPE(StringData)
REALM_RESULTS_TYPE(BinaryData)
REALM_RESULTS_TYPE(Timestamp)
REALM_RESULTS_TYPE(util::Optional<bool>)
REALM_RESULTS_TYPE(util::Optional<int64_t>)
REALM_RESULTS_TYPE(util::Optional<float>)
REALM_RESULTS_TYPE(util::Optional<double>)

#undef REALM_RESULTS_TYPE
}

Results::OutOfBoundsIndexException::OutOfBoundsIndexException(size_t r, size_t c)
: std::out_of_range(util::format("Requested index %1 greater than max %2", r, c))
, requested(r), valid_count(c) {}

Results::UnsupportedColumnTypeException::UnsupportedColumnTypeException(size_t column, const Table* table, const char* operation)
: std::logic_error(util::format("Cannot %1 property '%2': operation not supported for '%3' properties",
                                  operation, table->get_column_name(column),
                                  string_for_property_type(static_cast<PropertyType>(table->get_column_type(column)))))
, column_index(column)
, column_name(table->get_column_name(column))
, column_type(table->get_column_type(column))
{
}
