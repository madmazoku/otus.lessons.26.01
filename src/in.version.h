#pragma once

#cmakedefine PROJECT_VERSION_PATCH @PROJECT_VERSION_PATCH@

inline int build_version()
{
    return PROJECT_VERSION_PATCH;
}