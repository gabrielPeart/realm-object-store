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

#ifndef REALM_OS_OBJECT_ACCESSOR_IMPL_HPP
#define REALM_OS_OBJECT_ACCESSOR_IMPL_HPP

#include "object_accessor.hpp"

#include "util/any.hpp"

namespace realm {
using AnyDict = std::map<std::string, util::Any>;
using AnyVector = std::vector<util::Any>;

struct CppContext {
    util::Optional<util::Any> value_for_property(util::Any& dict,
                                                 const std::string &prop_name, size_t)
    {
        auto const& v = any_cast<AnyDict&>(dict);
        auto it = v.find(prop_name);
        return it == v.end() ? util::none : util::make_optional(it->second);
    }

    template<typename Func>
    void list_enumerate(util::Any& value, Func&& fn) {
        for (auto v : any_cast<AnyVector&>(value))
            fn(v);
    }

    util::Optional<util::Any>
    default_value_for_property(Realm*, ObjectSchema const&, std::string const&)
    {
        return util::none;
    }

    Timestamp to_timestamp(util::Any& v) { return any_cast<Timestamp>(v); }
    bool to_bool(util::Any& v) { return any_cast<bool>(v); }
    double to_double(util::Any& v) { return any_cast<double>(v); }
    float to_float(util::Any& v) { return any_cast<float>(v); }
    long long to_long(util::Any& v) { return any_cast<long long>(v); }
    std::string& to_binary(util::Any& v) { return any_cast<std::string&>(v); }
    StringData to_string(util::Any& v) { return any_cast<std::string&>(v).c_str(); }
    Mixed to_mixed(util::Any&) { throw std::logic_error("'Any' type is unsupported"); }

    util::Any from_binary(BinaryData v) { return std::string(v); }
    util::Any from_bool(bool v) { return v; }
    util::Any from_double(double v) { return v; }
    util::Any from_float(float v) { return v; }
    util::Any from_long(long long v) { return v; }
    util::Any from_string(StringData v) { return std::string(v); }
    util::Any from_timestamp(Timestamp v) { return v; }
    util::Any from_list(List v) { return v; }
    util::Any from_table(TableRef v) { return v; }
    util::Any from_results(Results v) { return v; }
    util::Any from_object(Object v) { return v; }

    bool is_null(util::Any const& v) { return !v.has_value(); }
    util::Any null_value() { return {}; }

    size_t to_object_index(SharedRealm realm, util::Any& value, std::string const& object_type, bool update)
    {
        if (auto object = any_cast<Object>(&value)) {
            return object->row().get_index();
        }

        return Object::create(*this, realm, *realm->schema().find(object_type), value, update).row().get_index();
    }

    void will_change(Object const&, Property const&) {}
    void did_change() {}
    std::string print(util::Any) { return "not implemented"; }
    bool allow_missing(util::Any) { return false; }
};
}

#endif /* REALM_OS_OBJECT_ACCESSOR_IMPL_HPP */
