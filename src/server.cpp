
#include <iostream>
#include <exception>
#include <map>

#include <boost/asio.hpp>
#include <boost/asio/spawn.hpp>
#include <boost/asio/signal_set.hpp>

#include "../bin/version.h"

#include "session.h"

int main(int argc, char** argv)
{
    try {
        if(argc != 2) {
            std::cerr << "Usage: " << argv[0] << " <port>" << std::endl;
            return 1;
        }

        Metrics m;
        std::map<size_t, std::string> a, b;

        boost::asio::io_service io;

        boost::asio::signal_set sigint(io, SIGINT);
        boost::asio::ip::tcp::acceptor acceptor(io, boost::asio::ip::tcp::endpoint(boost::asio::ip::tcp::v4(), std::atoi(argv[1])));

        sigint.async_wait(
        [&](boost::system::error_code ec, int signal) {
            std::cerr << "finish" << std::endl;
            acceptor.close();
        });

        boost::asio::spawn(io,
        [&](boost::asio::yield_context yield) {
            boost::system::error_code ec;
            while (listen) {
                boost::asio::ip::tcp::socket socket(io);
                acceptor.async_accept(socket, yield[ec]);
                if (ec) {
                    if(ec == boost::asio::error::operation_aborted)
                        break;
                    std::cerr << "accept error: " << ec;
                    break;
                }
                std::make_shared<Session>(std::move(socket), a, b, m)->go();
            }
        });


        io.run();

        m.dump("join_server", std::cout);

    } catch(std::exception& e) {
        std::cerr << e.what() << std::endl;
    }

    return 0;
}
