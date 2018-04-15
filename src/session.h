#pragma once

#include <memory>
#include <exception>
#include <algorithm>
#include <iterator>
#include <array>
#include <vector>

#include <boost/asio/spawn.hpp>
#include <boost/tokenizer.hpp>
#include <boost/algorithm/string.hpp>

#include "metrics.h"

class Session : public std::enable_shared_from_this<Session>
{
private:
    enum class CrossType {
        INTERSECTION,
        SYMMETRIC_DIFFERENCE
    };

    boost::asio::ip::tcp::socket _socket;
    boost::asio::io_service::strand _strand;

    boost::asio::ip::tcp::endpoint _remote;

    std::map<size_t, std::string>& _a;
    std::map<size_t, std::string>& _b;

    std::array<char, 8192> _buffer;
    std::string _data;
    Metrics& _m;

    bool _echo_cmd;
    bool _local_print_cmd;

    void cross(CrossType ct, boost::asio::yield_context& yield)
    {
        auto it_a = _a.begin();
        auto it_b = _b.begin();

        std::string line;

        bool has_a = it_a != _a.end();
        bool has_b = it_b != _b.end();

        while(it_a != _a.end() || has_b) {
            size_t cur_a = has_a ? it_a->first : 0;
            size_t cur_b = has_b ? it_b->first : 0;

            line.clear();
            switch(ct) {
            case CrossType::INTERSECTION:
                if(has_a && has_b && cur_a == cur_b)
                    line = std::to_string(it_a->first) + "\t" + it_a->second + "\t" + std::to_string(it_b->first) + "\t" + it_b->second;
                break;
            case CrossType::SYMMETRIC_DIFFERENCE:
                if(has_a && has_b)
                    if(cur_a < cur_b)
                        line = std::to_string(it_a->first) + "\t" + it_a->second + "\t\t";
                    else if(cur_a > cur_b)
                        line = "\t\t" + std::to_string(it_b->first) + "\t" + it_b->second;
                    else
                        ;
                else if(has_a)
                    line = std::to_string(it_a->first) + "\t" + it_a->second + "\t\t";
                else
                    line = "\t\t" + std::to_string(it_b->first) + "\t" + it_b->second;
            }

            if(line.empty())
                _strand.post(yield);
            else {
                line += "\n";
                boost::asio::async_write(_socket, boost::asio::buffer(line.c_str(), line.length()), yield);
            }

            it_a = has_a ? _a.find(cur_a) : _a.end();
            it_b = has_b ? _b.find(cur_b) : _b.end();

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

            has_a = it_a != _a.end();
            has_b = it_b != _b.end();
        }
    }

    void dump(std::map<size_t, std::string>& r, boost::asio::yield_context& yield)
    {
        std::string line;
        auto it = r.begin();

        while(it != r.end()) {
            size_t id = it->first;
            line = std::to_string(id) + "\t" + it->second + "\n";
            boost::asio::async_write(_socket, boost::asio::buffer(line.c_str(), line.length()), yield);
            it = r.find(id);
            ++it;
        }
    }

    void process_line(size_t start, size_t length, boost::asio::yield_context& yield)
    {
        _m.update("session.lines", 1);

        if(_echo_cmd)
            boost::asio::async_write(_socket, boost::asio::buffer(_data.c_str() + start, length), yield);

        if(_local_print_cmd) {
            std::cout << _remote << " CMD> ";
            std::cout.write(_data.c_str() + start, length - 1);
            std::cout << "'" << std::endl;
        }

        std::vector<std::string> tokens;

        boost::char_separator<char> sep{" \n"};
        boost::tokenizer<boost::char_separator<char>> tok{_data.cbegin() + start, _data.cbegin() + start + length, sep};
        std::copy( tok.begin(), tok.end(), std::back_inserter(tokens) );

        std::string response;
        if(tokens.empty()) {
            _m.update("session.errors.empty", 1);
            response = "ERR no command";
        } else {
            boost::to_upper(tokens[0]);
            if(tokens[0] == "INSERT") {
                response = validate_insert(tokens);
                if(response.empty()) {
                    std::map<size_t, std::string>& r = tokens[1] == "A" ?  _a : _b;
                    size_t id = std::stoull(tokens[2]);
                    auto f = r.find(id);
                    if(f == r.end()) {
                        r[id] = tokens[3];
                        _m.update("session.inserts", 1);
                        _m.update("session."+tokens[1]+".inserts", 1);
                        response = "OK";
                    } else {
                        _m.update("session.errors.insert", 1);
                        response = "ERR duplicate " + std::to_string(id);
                    }
                } else
                    _m.update("session.errors.insert", 1);
            } else if(tokens[0] == "TRUNCATE") {
                response = validate_truncate(tokens);
                if(response.empty()) {
                    _m.update("session.truncates", 1);
                    _m.update("session."+tokens[1]+".truncates", 1);
                    std::map<size_t, std::string>& r = tokens[1] == "A" ?  _a : _b;
                    r.clear();
                    response = "OK";
                } else
                    _m.update("session.errors.insert", 1);
            } else if(tokens[0] == "INTERSECTION") {
                _m.update("session.intersections", 1);
                cross(CrossType::INTERSECTION, yield);
                response = "OK";
            } else if(tokens[0] == "SYMMETRIC_DIFFERENCE") {
                _m.update("session.symmetric_differencies", 1);
                cross(CrossType::SYMMETRIC_DIFFERENCE, yield);
                response = "OK";
            } else if(tokens[0] == "DUMP") {
                response = validate_dump(tokens);
                if(response.empty()) {
                    _m.update("session.dumps", 1);
                    _m.update("session."+tokens[1]+".dumps", 1);
                    std::map<size_t, std::string>& r = tokens[1] == "A" ?  _a : _b;
                    dump(r, yield);
                    response = "OK";
                } else
                    _m.update("session.errors.dump", 1);
            } else if(tokens[0] == "REMOVE") {
                response = validate_remove(tokens);
                if(response.empty()) {
                    std::map<size_t, std::string>& r = tokens[1] == "A" ?  _a : _b;
                    size_t id = std::stoull(tokens[2]);
                    auto f = r.find(id);
                    if(f != r.end()) {
                        r.erase(f);
                        _m.update("session.removes", 1);
                        _m.update("session."+tokens[1]+".removes", 1);
                        response = "OK";
                    } else {
                        _m.update("session.errors.remove", 1);
                        response = "ERR absent " + std::to_string(id);
                    }
                } else
                    _m.update("session.errors.insert", 1);
            } else if(tokens[0] == "HELP") {
                response = "INSERT table id desc - insert record {id, desc} to table, where table may be 'A' or 'B', id must be positive number and desc is a string\n";
                response += "TRUNCATE table - remove all records from table, where table may be 'A' or 'B'\n";
                response += "INTERSECTION - print records which id present in both tables 'A' and 'B'\n";
                response += "SYMMETRIC_DIFFERENCE - print records which id present only in one table - 'A' or 'B'\n";
                response += "DUMP table - print content of table, where table may be 'A' or 'B'\n";
                response += "REMOVE table id - remove existing record with id from table, where table may be 'A' or 'B' and id must be positive number\n";
                response += "HELP print this text\n";
            } else {
                _m.update("session.errors.unknown", 1);
                response = "ERR unknown command";
            }
        }

        response += "\n";
        boost::asio::async_write(_socket, boost::asio::buffer(response, response.length()), yield);
    }

    void to_upper(std::vector<std::string>& tokens, size_t n)
    {
        std::string r;
        r.reserve(tokens[n].size());
        for(auto c : tokens[n])
            r.push_back(std::toupper(c));
        tokens[n] = r;
    }

    bool is_num(const std::string& s)
    {
        if(s.empty())
            return false;
        for(auto c : s)
            if(!std::isdigit(c))
                return false;
        return true;
    }

    std::string validate_insert(std::vector<std::string>& tokens)
    {
        std::string response;
        if(tokens.size() < 4)
            response = "ERR not enough arguments for insert";
        else {
            boost::to_upper(tokens[1]);
            if(tokens[1] != "A" && tokens[1] != "B")
                response = "ERR table may be 'A' or 'B' only";
            else if(!is_num(tokens[2]))
                response = "ERR id must be number";
        }
        return std::move(response);
    }

    std::string validate_truncate(std::vector<std::string>& tokens)
    {
        std::string response;
        if(tokens.size() < 2)
            response = "ERR not enough arguments for truncate";
        else {
            boost::to_upper(tokens[1]);
            if(tokens[1] != "A" && tokens[1] != "B")
                response = "ERR table may be 'A' or 'B' only";
        }
        return std::move(response);
    }

    std::string validate_dump(std::vector<std::string>& tokens)
    {
        std::string response;
        if(tokens.size() < 2)
            response = "ERR not enough arguments for dump";
        else {
            boost::to_upper(tokens[1]);
            if(tokens[1] != "A" && tokens[1] != "B")
                response = "ERR table may be 'A' or 'B' only";
        }
        return std::move(response);
    }

    std::string validate_remove(std::vector<std::string>& tokens)
    {
        std::string response;
        if(tokens.size() < 3)
            response = "ERR not enough arguments for remove";
        else {
            boost::to_upper(tokens[1]);
            if(tokens[1] != "A" && tokens[1] != "B")
                response = "ERR table may be 'A' or 'B' only";
            else if(!is_num(tokens[2]))
                response = "ERR id must be number";
        }
        return std::move(response);
    }

    void process_data(boost::asio::yield_context& yield)
    {
        _m.update("session.reads", 1);

        size_t start_pos = 0;
        while(true) {
            size_t end_pos = _data.find('\n', start_pos);
            if(end_pos != std::string::npos) {
                process_line(start_pos, end_pos - start_pos + 1, yield);

                start_pos = end_pos;
                ++start_pos;
            } else {
                _data.erase(0, start_pos);
                break;
            }
        }
    }

public:
    explicit Session(boost::asio::ip::tcp::socket socket, std::map<size_t, std::string>& a, std::map<size_t, std::string>& b, Metrics& m)
        : _socket(std::move(socket)),
          _strand(_socket.get_io_service()),
          _a(a),
          _b(b),
          _m(m),
          _echo_cmd(false),
          _local_print_cmd(false)
    {
        _m.update("session.count", 1);

        boost::system::error_code ec;
        _remote = std::move(_socket.remote_endpoint(ec));

        if(_local_print_cmd) {
            std::cout << "New session: " << _remote << std::endl;
        }
    }

    void go()
    {
        auto self(shared_from_this());
        boost::asio::spawn(_strand,
        [this, self](boost::asio::yield_context yield) {

            std::string response;

            boost::system::error_code ec;
            while(true) {
                std::size_t length = _socket.async_read_some(boost::asio::buffer(_buffer.data(), _buffer.size()), yield[ec]);
                if (ec) {
                    if(ec == boost::asio::error::eof || ec == boost::asio::error::connection_reset)
                        break;
                    std::cerr << _remote << " read error: " << ec;
                    break;
                }

                _data.append(_buffer.data(), length);
                process_data(yield);
            }
        });
    }
};