#pragma once

#include <boost/asio.hpp>

#include "metrics.h"

bool is_num(const std::string& s)
{
    if(s.empty())
        return false;
    for(auto c : s)
        if(!std::isdigit(c))
            return false;
    return true;
}

class CommandState
{
public:
    Metrics& _m;
    std::map<size_t, std::string>& _a;
    std::map<size_t, std::string>& _b;

    boost::asio::ip::tcp::socket& _socket;
    boost::asio::io_service::strand& _strand;

    CommandState(
        Metrics& m,
        std::map<size_t, std::string>& a,
        std::map<size_t, std::string>& b,
        boost::asio::ip::tcp::socket& socket,
        boost::asio::io_service::strand& strand
    ) : _m(m), _a(a), _b(b), _socket(socket), _strand(strand)
    {
    }
};

class Command
{
public:
    virtual std::string name() = 0;
    virtual std::string validate(std::vector<std::string>& tokens) = 0;
    virtual std::string execute(std::vector<std::string>& tokens, boost::asio::yield_context& yield) = 0;

    virtual ~Command() = default;
};

using Commands = std::map<std::string, std::unique_ptr<Command>>;

class CInsert : public Command
{
private:
    CommandState _s;

public:
    CInsert(CommandState& s) : _s(s) {}

    virtual std::string name() final { return "INSERT"; }
    virtual std::string validate(std::vector<std::string>& tokens) final {
        std::string response;
        if(tokens.size() < 4)
            response = "ERR not enough arguments for insert";
        else
        {
            boost::to_upper(tokens[1]);
            if(tokens[1] != "A" && tokens[1] != "B")
                response = "ERR table may be 'A' or 'B' only";
            else if(!is_num(tokens[2]))
                response = "ERR id must be number";
        }
        return std::move(response);
    }
    virtual std::string execute(std::vector<std::string>& tokens, boost::asio::yield_context& yield) final {
        std::string response;

        std::map<size_t, std::string>& r = tokens[1] == "A" ?  _s._a : _s._b;
        size_t id = std::stoull(tokens[2]);
        auto f = r.find(id);
        if(f == r.end())
        {
            r[id] = tokens[3];
            _s._m.update("session.successes." + name(), 1);
            _s._m.update("session.successes."+tokens[1]+"."+name(), 1);
        } else
            response = "ERR duplicate " + std::to_string(id);

        return std::move(response);
    }
};

class CTruncate : public Command
{
private:
    CommandState _s;

public:
    CTruncate(CommandState& s) : _s(s) {}

    virtual std::string name() final { return "TRUNCATE"; }
    virtual std::string validate(std::vector<std::string>& tokens) final {
        std::string response;
        if(tokens.size() < 2)
            response = "ERR not enough arguments for truncate";
        else
        {
            boost::to_upper(tokens[1]);
            if(tokens[1] != "A" && tokens[1] != "B")
                response = "ERR table may be 'A' or 'B' only";
        }
        return std::move(response);
    }
    virtual std::string execute(std::vector<std::string>& tokens, boost::asio::yield_context& yield) final {
        std::string response;

        _s._m.update("session.successes." + name(), 1);
        _s._m.update("session.successes."+tokens[1]+"."+name(), 1);

        boost::system::error_code ec;
        std::map<size_t, std::string>& r = tokens[1] == "A" ?  _s._a : _s._b;
        while(!r.empty())
        {
            r.erase(r.begin());
            _s._strand.post(yield[ec]);
            if(ec) {
                response = "session error";
                std::cerr << "session error: " << ec << std::endl;
                break;
            }
        }

        return std::move(response);
    }
};

class CCross : public Command
{
private:
    CommandState _s;

    virtual std::string cross(bool has_a, bool has_b, std::map<size_t, std::string>::iterator& it_a, std::map<size_t, std::string>::iterator& it_b) = 0;

public:
    CCross(CommandState& s) : _s(s) {}

    virtual std::string validate(std::vector<std::string>& tokens) final {
        std::string response;
        return std::move(response);
    }
    virtual std::string execute(std::vector<std::string>& tokens, boost::asio::yield_context& yield) final {
        std::string response;

        _s._m.update("session.successes." + name(), 1);

        auto it_a = _s._a.begin();
        auto it_b = _s._b.begin();

        std::string line;

        bool has_a = it_a != _s._a.end();
        bool has_b = it_b != _s._b.end();

        boost::system::error_code ec;

        while(has_a || has_b)
        {
            size_t cur_a = has_a ? it_a->first : 0;
            size_t cur_b = has_b ? it_b->first : 0;

            line = cross(has_a, has_b, it_a, it_b);

            if(line.empty())
                _s._strand.post(yield[ec]);
            else {
                line += "\n";
                boost::asio::async_write(_s._socket, boost::asio::buffer(line.c_str(), line.length()), yield[ec]);
            }

            if(ec) {
                response = "session error";
                std::cerr << "session error: " << ec << std::endl;
                break;
            }

            it_a = has_a ? _s._a.find(cur_a) : _s._a.end();
            it_b = has_b ? _s._b.find(cur_b) : _s._b.end();

            if(has_a && has_b)
                if(cur_a == cur_b) {
                    ++it_a;
                    ++it_b;
                } else if(cur_a < cur_b)
                    ++it_a;
                else
                    ++it_b;
            else if(has_a)
                ++it_a;
            else
                ++it_b;

            has_a = it_a != _s._a.end();
            has_b = it_b != _s._b.end();
        }

        return std::move(response);
    }
};

class CCIntersection : public CCross
{
private:
    virtual std::string cross(bool has_a, bool has_b, std::map<size_t, std::string>::iterator& it_a, std::map<size_t, std::string>::iterator& it_b) final {
        std::string line;
        if(has_a && has_b && it_a->first == it_b->first)
            line = std::to_string(it_a->first) + "\t" + it_a->second + "\t" + std::to_string(it_b->first) + "\t" + it_b->second;
        return std::move(line);
    }

public:
    CCIntersection(CommandState& s) : CCross(s) {}

    virtual std::string name() final { return "INTERSECTION"; }

};

class CCSymmetricDifference : public CCross
{
private:
    virtual std::string cross(bool has_a, bool has_b, std::map<size_t, std::string>::iterator& it_a, std::map<size_t, std::string>::iterator& it_b) final {
        std::string line;
        if(has_a && has_b)
            if(it_a->first < it_b->first)
                line = std::to_string(it_a->first) + "\t" + it_a->second + "\t\t";
            else if(it_a->first > it_b->first)
                line = "\t\t" + std::to_string(it_b->first) + "\t" + it_b->second;
            else
                ;
        else if(has_a)
            line = std::to_string(it_a->first) + "\t" + it_a->second + "\t\t";
        else
            line = "\t\t" + std::to_string(it_b->first) + "\t" + it_b->second;
        return std::move(line);
    }

public:
    CCSymmetricDifference(CommandState& s) : CCross(s) {}

    virtual std::string name() final { return "SYMMETRIC_DIFFERENCE"; }

};

class CRemove : public Command
{
private:
    CommandState _s;

public:
    CRemove(CommandState& s) : _s(s) {}

    virtual std::string name() final { return "REMOVE"; }
    virtual std::string validate(std::vector<std::string>& tokens) final {
        std::string response;
        if(tokens.size() < 3)
            response = "ERR not enough arguments for remove";
        else
        {
            boost::to_upper(tokens[1]);
            if(tokens[1] != "A" && tokens[1] != "B")
                response = "ERR table may be 'A' or 'B' only";
            else if(!is_num(tokens[2]))
                response = "ERR id must be number";
        }
        return std::move(response);
    }
    virtual std::string execute(std::vector<std::string>& tokens, boost::asio::yield_context& yield) final {
        std::string response;

        std::map<size_t, std::string>& r = tokens[1] == "A" ?  _s._a : _s._b;
        size_t id = std::stoull(tokens[2]);
        auto f = r.find(id);
        if(f != r.end())
        {
            r.erase(f);
            _s._m.update("session.successes." + name(), 1);
            _s._m.update("session.successes."+tokens[1]+"."+name(), 1);
        } else
            response = "ERR absent " + std::to_string(id);

        return std::move(response);
    }
};

class CDump : public Command
{
private:
    CommandState _s;

public:
    CDump(CommandState& s) : _s(s) {}

    virtual std::string name() final { return "DUMP"; }
    virtual std::string validate(std::vector<std::string>& tokens) final {
        std::string response;
        if(tokens.size() < 2)
            response = "ERR not enough arguments for dump";
        else
        {
            boost::to_upper(tokens[1]);
            if(tokens[1] != "A" && tokens[1] != "B")
                response = "ERR table may be 'A' or 'B' only";
        }
        return std::move(response);
    }
    virtual std::string execute(std::vector<std::string>& tokens, boost::asio::yield_context& yield) final {
        std::string response;
        _s._m.update("session.successes." + name(), 1);
        _s._m.update("session.successes."+tokens[1]+"."+name(), 1);

        std::string line;
        std::map<size_t, std::string>& r = tokens[1] == "A" ?  _s._a : _s._b;

        boost::system::error_code ec;

        auto it = r.begin();
        while(it != r.end())
        {
            size_t id = it->first;
            line = std::to_string(id) + "\t" + it->second + "\n";
            boost::asio::async_write(_s._socket, boost::asio::buffer(line.c_str(), line.length()), yield[ec]);

            if(ec) {
                response = "session error";
                std::cerr << "session error: " << ec << std::endl;
                break;
            }

            it = r.find(id);
            ++it;
        }

        return std::move(response);
    }
};

class CHelp : public Command
{
private:
    CommandState _s;

public:
    CHelp(CommandState& s) : _s(s) {}

    virtual std::string name() final { return "HELP"; }
    virtual std::string validate(std::vector<std::string>& tokens) final {
        std::string response;
        return std::move(response);
    }
    virtual std::string execute(std::vector<std::string>& tokens, boost::asio::yield_context& yield) final {
        std::string response;
        _s._m.update("session.successes." + name(), 1);

        std::vector<std::string> helps;
        helps.push_back("INSERT table id desc - insert record {id, desc} to table, where table may be 'A' or 'B', id must be positive number and desc is a string\n");
        helps.push_back("TRUNCATE table - remove all records from table, where table may be 'A' or 'B'\n");
        helps.push_back("INTERSECTION - print records which id present in both tables 'A' and 'B'\n");
        helps.push_back("SYMMETRIC_DIFFERENCE - print records which id present only in one table - 'A' or 'B'\n");
        helps.push_back("DUMP table - print content of table, where table may be 'A' or 'B'\n");
        helps.push_back("REMOVE table id - remove existing record with id from table, where table may be 'A' or 'B' and id must be positive number\n");
        helps.push_back("HELP print this text\n");

        boost::system::error_code ec;
        for(auto& h : helps)
        {
            boost::asio::async_write(_s._socket, boost::asio::buffer(h.c_str(), h.length()), yield[ec]);
            if(ec) {
                response = "session error";
                std::cerr << "session error: " << ec << std::endl;
                break;
            }
        }

        return std::move(response);
    }
};
