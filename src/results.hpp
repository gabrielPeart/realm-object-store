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

#include "collection_notifications.hpp"
#include "impl/collection_notifier.hpp"
#include "property.hpp"

#include <realm/table_view.hpp>
#include <realm/util/optional.hpp>

namespace realm {
template<typename T> class BasicRowExpr;
using RowExpr = BasicRowExpr<Table>;
class Mixed;
class ObjectSchema;

namespace _impl {
    class ResultsNotifier;
}

class Results {
public:
    // Results can be either be backed by nothing, a thin wrapper around a table,
    // or a wrapper around a query and a sort order which creates and updates
    // the tableview as needed
    Results();
    Results(std::shared_ptr<Realm> r, Table& table);
    Results(std::shared_ptr<Realm> r, Query q, SortDescriptor s = {}, SortDescriptor d = {});
    Results(std::shared_ptr<Realm> r, TableView tv, SortDescriptor s = {}, SortDescriptor d = {});
    Results(std::shared_ptr<Realm> r, LinkViewRef lv, util::Optional<Query> q = {}, SortDescriptor s = {});
    ~Results();

    // Results is copyable and moveable
    Results(Results&&);
    Results& operator=(Results&&);
    Results(const Results&);
    Results& operator=(const Results&);

    // Get the Realm
    std::shared_ptr<Realm> get_realm() const { return m_realm; }

    // Object schema describing the vendored object type
    const ObjectSchema &get_object_schema() const;

    // Get a query which will match the same rows as is contained in this Results
    // Returned query will not be valid if the current mode is Empty
    Query get_query() const;

    // Get the currently applied sort order for this Results
    SortDescriptor const& get_sort() const noexcept { return m_sort; }

    // Get the currently applied distinct condition for this Results
    SortDescriptor const& get_distinct() const noexcept { return m_distinct; }

    // Get a tableview containing the same rows as this Results
    TableView get_tableview();

    // Get the object type which will be returned by get()
    StringData get_object_type() const noexcept;

    PropertyType get_type() const;
    bool is_optional() const noexcept;

    // Get the LinkView this Results is derived from, if any
    LinkViewRef get_linkview() const { return m_link_view; }

    // Get the size of this results
    // Can be either O(1) or O(N) depending on the state of things
    size_t size();

    // Get the row accessor for the given index
    // Throws OutOfBoundsIndexException if index >= size()
    template<typename T = RowExpr>
    T get(size_t index);

    // Get a row accessor for the first/last row, or none if the results are empty
    // More efficient than calling size()+get()
    template<typename T = RowExpr>
    util::Optional<T> first();
    template<typename T = RowExpr>
    util::Optional<T> last();

    // Get the first index of the given value in this results, or not_found
    template<typename T>
    size_t index_of(T const& value);

    // Delete all of the rows in this Results from the Realm
    // size() will always be zero afterwards
    // Throws InvalidTransactionException if not in a write transaction
    void clear();

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
    template<typename T = Mixed> util::Optional<T> max(size_t column);
    template<typename T = Mixed> util::Optional<T> min(size_t column);
    template<typename T = Mixed> util::Optional<T> average(size_t column);
    template<typename T = Mixed> util::Optional<T> sum(size_t column);

    enum class Mode {
        Empty, // Backed by nothing (for missing tables)
        Table, // Backed directly by a Table
        Query, // Backed by a query that has not yet been turned into a TableView
        LinkView,  // Backed directly by a LinkView
        TableView, // Backed by a TableView created from a Query
    };
    // Get the currrent mode of the Results
    // Ideally this would not be public but it's needed for some KVO stuff
    Mode get_mode() const { return m_mode; }

    // Is this Results associated with a Realm that has not been invalidated?
    bool is_valid() const;

    // The Results object has been invalidated (due to the Realm being invalidated)
    // All non-noexcept functions can throw this
    struct InvalidatedException : public std::logic_error {
        InvalidatedException() : std::logic_error("Access to invalidated Results objects") {}
    };

    // The input index parameter was out of bounds
    struct OutOfBoundsIndexException : public std::out_of_range {
        OutOfBoundsIndexException(size_t r, size_t c);
        const size_t requested;
        const size_t valid_count;
    };

    // The input Row object is not attached
    struct DetatchedAccessorException : public std::logic_error {
        DetatchedAccessorException() : std::logic_error("Atempting to access an invalid object") {}
    };

    // The input Row object belongs to a different table
    struct IncorrectTableException : public std::logic_error {
        IncorrectTableException(StringData e, StringData a, const std::string &error) :
            std::logic_error(error), expected(e), actual(a) {}
        const StringData expected;
        const StringData actual;
    };

    // The requested aggregate operation is not supported for the column type
    struct UnsupportedColumnTypeException : public std::logic_error {
        size_t column_index;
        StringData column_name;
        DataType column_type;

        UnsupportedColumnTypeException(size_t column, const Table* table, const char* operation);
    };

    // Create an async query from this Results
    // The query will be run on a background thread and delivered to the callback,
    // and then rerun after each commit (if needed) and redelivered if it changed
    NotificationToken async(std::function<void (std::exception_ptr)> target);
    NotificationToken add_notification_callback(CollectionChangeCallback cb) &;

    bool wants_background_updates() const { return m_wants_background_updates; }

    // Returns whether the rows are guaranteed to be in table order.
    bool is_in_table_order() const;

    // Helper type to let ResultsNotifier update the tableview without giving access
    // to any other privates or letting anyone else do so
    class Internal {
        friend class _impl::ResultsNotifier;
        static void set_table_view(Results& results, TableView&& tv);
    };

    template<typename Context> auto get(Context&, size_t index);
    template<typename Context> auto first(Context&);
    template<typename Context> auto last(Context&);

    template<typename Context, typename T>
    size_t index_of(Context&, T value);

    template<typename Context> auto max(Context&);
    template<typename Context> auto min(Context&);
    template<typename Context> auto average(Context&);
    template<typename Context> auto sum(Context&);

private:
    enum class UpdatePolicy {
        Auto,  // Update automatically to reflect changes in the underlying data.
        Never, // Never update.
    };

    std::shared_ptr<Realm> m_realm;
    mutable const ObjectSchema *m_object_schema = nullptr;
    Query m_query;
    TableView m_table_view;
    LinkViewRef m_link_view;
    TableRef m_table;
    SortDescriptor m_sort;
    SortDescriptor m_distinct;

    _impl::CollectionNotifier::Handle<_impl::ResultsNotifier> m_notifier;

    Mode m_mode = Mode::Empty;
    UpdatePolicy m_update_policy = UpdatePolicy::Auto;
    bool m_has_used_table_view = false;
    bool m_wants_background_updates = true;

    void update_tableview(bool wants_notifications = true);
    bool update_linkview();

    void validate_read() const;
    void validate_write() const;

    void prepare_async();

    template<typename Int, typename Float, typename Double, typename Timestamp>
    util::Optional<Mixed> aggregate(size_t column,
                                    const char* name,
                                    Int agg_int, Float agg_float,
                                    Double agg_double, Timestamp agg_timestamp);

    void set_table_view(TableView&& tv);

    template<typename Fn>
    auto dispatch(Fn&&) const;

    template<typename T>
    T do_get(size_t row);
};

template<typename Fn>
auto Results::dispatch(Fn&& fn) const
{
    using Type = PropertyType;
    switch (get_type()) {
        case Type::Int:    return is_optional() ? fn((util::Optional<int64_t>*)0) : fn((int64_t*)0);
        case Type::Bool:   return is_optional() ? fn((util::Optional<bool>*)0)    : fn((bool*)0);
        case Type::Float:  return is_optional() ? fn((util::Optional<float>*)0)   : fn((float*)0);
        case Type::Double: return is_optional() ? fn((util::Optional<double>*)0)  : fn((double*)0);
        case Type::String: return fn((StringData*)0);
        case Type::Data:   return fn((BinaryData*)0);
        case Type::Object: return fn((RowExpr*)0);
        case Type::Date:   return fn((Timestamp*)0);
        default: REALM_COMPILER_HINT_UNREACHABLE();
    }
}

template<typename Context>
auto Results::get(Context& ctx, size_t row_ndx)
{
    return dispatch([&](auto t) { return ctx.box(get<std::decay_t<decltype(*t)>>(row_ndx)); });
}

template<typename Context>
auto Results::first(Context& ctx)
{
    return dispatch([&](auto t) {
        auto value = first<std::decay_t<decltype(*t)>>();
        return value ? ctx.box(*value) : ctx.no_value();
    });
}

template<typename Context>
auto Results::last(Context& ctx)
{
    return dispatch([&](auto t) {
        auto value = last<std::decay_t<decltype(*t)>>();
        return value ? ctx.box(*value) : ctx.no_value();
    });
}

template<typename Context, typename T>
size_t Results::index_of(Context& ctx, T value)
{
    return dispatch([&](auto t) { return index_of(ctx.template unbox<std::decay_t<decltype(*t)>>(value)); });
}

template<typename Context>
auto Results::max(Context& ctx)
{
    return dispatch([&](auto t) { return ctx.box(max<std::decay_t<decltype(*t)>>(0)); });
}

template<typename Context>
auto Results::min(Context& ctx)
{
    return dispatch([&](auto t) { return ctx.box(min<std::decay_t<decltype(*t)>>(0)); });
}

template<typename Context>
auto Results::average(Context& ctx)
{
    return dispatch([&](auto t) { return ctx.box(average<std::decay_t<decltype(*t)>>(0)); });
}

template<typename Context>
auto Results::sum(Context& ctx)
{
    return dispatch([&](auto t) { return ctx.box(sum<std::decay_t<decltype(*t)>>(0)); });
}
} // namespace realm

#endif // REALM_RESULTS_HPP
