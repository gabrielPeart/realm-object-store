////////////////////////////////////////////////////////////////////////////
//
// Copyright 2017 Realm Inc.
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

#include "impl/results_base.hpp"

#include "impl/realm_coordinator.hpp"
#include "impl/results_notifier.hpp"
#include "object_store.hpp"
#include "util/compiler.hpp"
#include "util/format.hpp"

#include <stdexcept>

using namespace realm;
using namespace realm::_impl;

ResultsBase::ResultsBase() = default;
ResultsBase::~ResultsBase() = default;

ResultsBase::ResultsBase(SharedRealm r, Query q, SortDescriptor s, SortDescriptor d)
: m_realm(std::move(r))
, m_query(std::move(q))
, m_table(m_query.get_table())
, m_sort(std::move(s))
, m_distinct(std::move(d))
, m_mode(Mode::Query)
{
}

ResultsBase::ResultsBase(SharedRealm r, Table& table)
: m_realm(std::move(r))
, m_mode(Mode::Table)
{
    m_table.reset(&table);
}

ResultsBase::ResultsBase(SharedRealm r, LinkViewRef lv, util::Optional<Query> q, SortDescriptor s)
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

ResultsBase::ResultsBase(SharedRealm r, TableView tv, SortDescriptor s, SortDescriptor d)
: m_realm(std::move(r))
, m_table_view(std::move(tv))
, m_sort(std::move(s))
, m_distinct(std::move(d))
, m_mode(Mode::TableView)
{
    m_table.reset(&m_table_view.get_parent());
}

ResultsBase::ResultsBase(const ResultsBase&) = default;
ResultsBase& ResultsBase::operator=(const ResultsBase&) = default;

ResultsBase::ResultsBase(ResultsBase&& other)
: m_realm(std::move(other.m_realm))
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

ResultsBase& ResultsBase::operator=(ResultsBase&& other)
{
    this->~ResultsBase();
    new (this) ResultsBase(std::move(other));
    return *this;
}

bool ResultsBase::is_valid() const
{
    if (m_realm)
        m_realm->verify_thread();

    if (m_table && !m_table->is_attached())
        return false;

    return true;
}

void ResultsBase::validate_read() const
{
    // is_valid ensures that we're on the correct thread.
    if (!is_valid())
        throw InvalidatedException();
}

void ResultsBase::validate_write() const
{
    validate_read();
    if (!m_realm || !m_realm->is_in_transaction())
        throw InvalidTransactionException("Must be in a write transaction");
}

size_t ResultsBase::size()
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

bool ResultsBase::update_linkview()
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

void ResultsBase::update_tableview(bool wants_notifications)
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

void ResultsBase::clear()
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
                    // Copy the TableView because a frozen ResultsBase shouldn't let its size() change.
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

Query ResultsBase::get_query() const
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

TableView ResultsBase::get_tableview()
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

void ResultsBase::prepare_async()
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
        throw std::logic_error("Cannot create asynchronous query for snapshotted ResultsBase.");
    }

    m_wants_background_updates = true;
    m_notifier = std::make_shared<_impl::ResultsNotifier>(*this);
    _impl::RealmCoordinator::register_notifier(m_notifier);
}

NotificationToken ResultsBase::async(std::function<void (std::exception_ptr)> target)
{
    prepare_async();
    auto wrap = [=](CollectionChangeSet, std::exception_ptr e) { target(e); };
    return {m_notifier, m_notifier->add_callback(wrap)};
}

NotificationToken ResultsBase::add_notification_callback(CollectionChangeCallback cb) &
{
    prepare_async();
    return {m_notifier, m_notifier->add_callback(std::move(cb))};
}

bool ResultsBase::is_in_table_order() const
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

void ResultsBase::switch_to_query()
{
    m_query = get_query();
    m_mode = Mode::Query;
}

void ResultsBase::snapshot()
{
    validate_read();

    switch (get_mode()) {
        case Mode::Empty:
            break;
        case Mode::Table:
        case Mode::LinkView:
            switch_to_query();
            REALM_FALLTHROUGH;
        case Mode::Query:
        case Mode::TableView:
            update_tableview(false);
            m_notifier.reset();
            m_update_policy = UpdatePolicy::Never;
    }
}

void ResultsBase::Internal::set_table_view(ResultsBase& results, realm::TableView &&tv)
{
    REALM_ASSERT(results.m_update_policy != UpdatePolicy::Never);
    // If the previous TableView was never actually used, then stop generating
    // new ones until the user actually uses the ResultsBase object again
    if (results.m_mode == Mode::TableView) {
        results.m_wants_background_updates = results.m_has_used_table_view;
    }

    results.m_table_view = std::move(tv);
    results.m_mode = Mode::TableView;
    results.m_has_used_table_view = false;
    REALM_ASSERT(results.m_table_view.is_in_sync());
    REALM_ASSERT(results.m_table_view.is_attached());
}

ResultsBase::OutOfBoundsIndexException::OutOfBoundsIndexException(size_t r, size_t c)
: std::out_of_range(util::format("Requested index %1 greater than max %2", r, c))
, requested(r), valid_count(c) {}

ResultsBase::UnsupportedColumnTypeException::UnsupportedColumnTypeException(size_t column,
                                                                        const Table* table,
                                                                        const char* operation)
: std::logic_error(util::format("Cannot %1 property '%2': operation not supported for '%3' properties",
                                  operation, table->get_column_name(column),
                                  string_for_property_type(static_cast<PropertyType>(table->get_column_type(column)))))
, column_index(column)
, column_name(table->get_column_name(column))
, column_type(table->get_column_type(column))
{
}
