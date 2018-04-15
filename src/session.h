#pragma once

#include <memory>
#include <exception>
#include <algorithm>
#include <iterator>
#include <array>
#include <vector>
#include <map>

#include <boost/asio/spawn.hpp>
#include <boost/tokenizer.hpp>
#include <boost/algorithm/string.hpp>

#include "metrics.h"
#include "command.h"

class Session : public std::enable_shared_from_this<Session>
{
private:
    Metrics& _m;

    boost::asio::ip::tcp::socket _socket;
    boost::asio::io_service::strand _strand;

    boost::asio::ip::tcp::endpoint _remote;

    std::map<size_t, std::string>& _a;
    std::map<size_t, std::string>& _b;

    std::array<char, 8192> _buffer;
    std::string _data;

    bool _echo_cmd;
    bool _local_print_cmd;

    CommandState _s;
    Commands _commands;

    void add_command(std::unique_ptr<Command> command)
    {
        _commands[command->name()] = std::move(command);
    }

    void process_line(size_t start, size_t length, boost::asio::yield_context& yield)
    {
        boost::system::error_code ec;

        _m.update("session.lines", 1);

        if(_echo_cmd) {
            boost::asio::async_write(_socket, boost::asio::buffer(_data.c_str() + start, length), yield[ec]);
            if(ec) {
                std::cerr << "sesion error: " << ec << std::endl;
                return;
            }
        }

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
            auto c = _commands.find(tokens[0]);
            if(c != _commands.end()) {
                response = c->second->validate(tokens);
                if(response.empty())
                    response = c->second->execute(tokens, yield);
                if(response.empty())
                    response = "OK";
                else
                    _m.update("session.errors." + tokens[0], 1);
            } else {
                _m.update("session.errors.unknown", 1);
                response = "ERR unknown command";
            }
        }

        response += "\n";
        boost::asio::async_write(_socket, boost::asio::buffer(response, response.length()), yield[ec]);
        if(ec) {
            std::cerr << "sesion error: " << ec << std::endl;
            return;
        }
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
        : _m(m),
          _socket(std::move(socket)),
          _strand(_socket.get_io_service()),
          _a(a),
          _b(b),
          _echo_cmd(false),
          _local_print_cmd(false),
          _s(m, a, b, _socket, _strand)
    {
        _m.update("session.count", 1);

        add_command(std::make_unique<CInsert>(_s));
        add_command(std::make_unique<CTruncate>(_s));
        add_command(std::make_unique<CCIntersection>(_s));
        add_command(std::make_unique<CCSymmetricDifference>(_s));
        add_command(std::make_unique<CRemove>(_s));
        add_command(std::make_unique<CDump>(_s));
        add_command(std::make_unique<CHelp>(_s));

        boost::system::error_code ec;
        _remote = std::move(_socket.remote_endpoint(ec));

        if(_local_print_cmd)
            std::cout << "New session: " << _remote << std::endl;
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