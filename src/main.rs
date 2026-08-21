use crate::networking::server::tcp::TCPServerCore;


mod util;
mod networking;

enum test {
    bla(u32),
}

impl test {

    fn u(&self) {
        
    }

}

fn main() {


    //let d = test::bla(12);


    TCPServerCore::run("127.0.0.1:25565".parse().unwrap());

}
