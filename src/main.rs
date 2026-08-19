use crate::networking::server::tcp::TCPServerCore;


mod util;
mod networking;

fn main() {


    TCPServerCore::run("127.0.0.1:25565".parse().unwrap());

}
