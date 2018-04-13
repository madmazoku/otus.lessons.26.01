#pragma once

#include <iostream>
#include <map>
#include <mutex>

using metrics_t = std::map<std::string, size_t>;

class Metrics
{
private:
    metrics_t _metrics;

    void _update(const std::string& metric, size_t increment)
    {
        if(increment > 0) {
            auto it_m = _metrics.find(metric);
            if(it_m != _metrics.end())
                it_m->second += increment;
            else
                _metrics[metric] = increment;
        }
    }

public:

    void update(metrics_t metrics)
    {
        for(auto &m : metrics)
            _update(m.first, m.second);
    }

    void update(const std::string& metric, size_t increment = 1)
    {
        _update(metric, increment);
    }

    void dump(const std::string& prefix = "", std::ostream& out = std::cout)
    {
        for(auto &m : _metrics) {
            if(!prefix.empty())
                out << prefix << '.';
            out << m.first << " = " << m.second << std::endl;
        }
    }
};

