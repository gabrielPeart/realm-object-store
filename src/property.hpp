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

#ifndef REALM_PROPERTY_HPP
#define REALM_PROPERTY_HPP

#include "util/compiler.hpp"
#include "util/tagged_bool.hpp"

#include <string>

#include <realm/data_type.hpp>

namespace realm {
enum class PropertyType : unsigned char {
    Int    = 0,
    Bool   = 1,
    String = 2,
    Data   = 3,
    Date   = 4,
    Float  = 5,
    Double = 6,
    Object = 7,
    LinkingObjects = 8, // Implies Array

    // deprecated and remains only for reading old files
    Any    = 9, 

    Indexed   = 32,
    Nullable  = 64,
    Array     = 128,
    Flags     = Indexed | Nullable | Array
};

template<typename E>
constexpr auto to_underlying(E e)
{
    return static_cast<typename std::underlying_type<E>::type>(e);
}

inline constexpr PropertyType operator&(PropertyType a, PropertyType b)
{
    return static_cast<PropertyType>(to_underlying(a) & to_underlying(b));
}

inline constexpr PropertyType operator|(PropertyType a, PropertyType b)
{
    return static_cast<PropertyType>(to_underlying(a) | to_underlying(b));
}

inline constexpr PropertyType operator^(PropertyType a, PropertyType b)
{
    return static_cast<PropertyType>(to_underlying(a) ^ to_underlying(b));
}

inline constexpr PropertyType operator~(PropertyType a)
{
    return static_cast<PropertyType>(~to_underlying(a));
}

inline constexpr bool operator==(PropertyType a, PropertyType b)
{
    return to_underlying(a & ~PropertyType::Flags) == to_underlying(b & ~PropertyType::Flags);
}

inline constexpr bool operator!=(PropertyType a, PropertyType b)
{
    return !(a == b);
}

inline constexpr bool is_array(PropertyType a)
{
    return to_underlying(a & PropertyType::Array) == to_underlying(PropertyType::Array);
}

inline PropertyType& operator&=(PropertyType & a, PropertyType b)
{
    a = a & b;
    return a;
}

inline PropertyType& operator|=(PropertyType & a, PropertyType b)
{
    a = a | b;
    return a;
}

inline PropertyType& operator^=(PropertyType & a, PropertyType b)
{
    a = a ^ b;
    return a;
}

static const char *string_for_property_type(PropertyType type);

struct Property {
    using IsPrimary = util::TaggedBool<class IsPrimaryTag>;
    using IsIndexed = util::TaggedBool<class IsIndexedTag>;
    using IsNullable = util::TaggedBool<class IsNullableTag>;
    
    std::string name;
    PropertyType type;
    std::string object_type;
    std::string link_origin_property_name;
    IsPrimary is_primary = false;
    IsIndexed is_indexed = false;
    IsNullable is_nullable = false;

    size_t table_column = -1;
    bool requires_index() const { return is_primary || is_indexed; }

    bool is_indexable() const
    {
        return type == PropertyType::Int
            || type == PropertyType::Bool
            || type == PropertyType::Date
            || type == PropertyType::String;
    }

    bool type_is_nullable() const
    {
        return !(is_array(type) && type == PropertyType::Object);
    }

    std::string type_string() const
    {
        if (is_array(type))
            return "array<" + object_type + ">";
        switch (type) {
            case PropertyType::Object:
                return "<" + object_type + ">";
            case PropertyType::LinkingObjects:
                return "linking objects<" + object_type + ">";
            default:
                return string_for_property_type(type);
        }
    }

#if REALM_NO_BRACED_INIT
    Property(std::string name="", PropertyType type=PropertyType::Int,
             std::string object_type="", std::string link_origin_property_name="",
             IsPrimary is_primary=false, IsIndexed is_indexed=false, IsNullable is_nullable=false)
    : name(std::move(name))
    , type(type)
    , object_type(std::move(object_type))
    , link_origin_property_name(std::move(link_origin_property_name))
    , is_primary(is_primary)
    , is_indexed(is_indexed)
    , is_nullable(is_nullable)
    {
    }
#endif
};

inline bool operator==(Property const& lft, Property const& rgt)
{
    // note: not checking table_column
    // ordered roughly by the cost of the check
    return lft.type == rgt.type
        && lft.is_primary == rgt.is_primary
        && lft.is_nullable == rgt.is_nullable
        && lft.requires_index() == rgt.requires_index()
        && lft.name == rgt.name
        && lft.object_type == rgt.object_type
        && lft.link_origin_property_name == rgt.link_origin_property_name;
}

static const char *string_for_property_type(PropertyType type)
{
    if (is_array(type)) {
        switch (type & ~PropertyType::Flags) {
            case PropertyType::String: return "[string]";
            case PropertyType::Int: return "[int]";
            case PropertyType::Bool: return "[bool]";
            case PropertyType::Date: return "[date]";
            case PropertyType::Data: return "[data]";
            case PropertyType::Double: return "[double]";
            case PropertyType::Float: return "[float]";
            case PropertyType::Object: return "[object]";
            case PropertyType::Any: return "[any]";
            case PropertyType::LinkingObjects: return "[linking objects]";
            default: REALM_COMPILER_HINT_UNREACHABLE();
        }
    }
    switch (type) {
        case PropertyType::String: return "string";
        case PropertyType::Int: return "int";
        case PropertyType::Bool: return "bool";
        case PropertyType::Date: return "date";
        case PropertyType::Data: return "data";
        case PropertyType::Double: return "double";
        case PropertyType::Float: return "float";
        case PropertyType::Object: return "object";
        case PropertyType::Any: return "any";
        case PropertyType::LinkingObjects: return "linking objects";
        default: REALM_COMPILER_HINT_UNREACHABLE();
    }
}
}

#endif /* REALM_PROPERTY_HPP */
