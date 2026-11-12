use std::time::Duration;

use arc_swap::cache;

use crate::{networking::packethandler::REGISTERED_PACKET_COUNT, util::now_ms};


const EMPTY_NODE: RateNode = RateNode { last_used: 0, cache_hit: 0, timeout_until: 0 };


pub struct RateNode {

    last_used: u64,
    cache_hit: usize,
    timeout_until: u64

}

impl RateNode {

    fn set_last_used(&mut self){
        self.last_used = now_ms()
    }

    pub fn set_timeout(&mut self, duration: Duration) {
        self.clear_hits();
        self.timeout_until = now_ms().saturating_add(duration.as_millis() as u64)
    }

    fn clear_hits(&mut self){
        self.cache_hit = 0
    }

    fn hit_cache(&mut self){
        self.cache_hit += 1
    }

    pub fn get_cache_hit(&self) -> usize {
        self.cache_hit
    }

    pub fn get_last_used(&self) -> u64{
        self.last_used
    }

    pub fn try_pass(&mut self, threshold: Duration) -> bool{

        
        if self.timeout_until > now_ms() {
            self.set_last_used();
            return false;
        }
        if self.last_used + threshold.as_millis() as u64 > now_ms() {
            self.hit_cache();
        }else{
            self.clear_hits();
        }
        self.set_last_used();
        return true;

    }

}

pub struct RateCache {
    cache: [RateNode; REGISTERED_PACKET_COUNT]
}

impl RateCache {
    
    pub fn new() -> Self{

        let cache = [EMPTY_NODE; REGISTERED_PACKET_COUNT];
        RateCache { cache }

    }

    pub fn get_node(&mut self, op_code: usize) -> Option<&mut RateNode> {
        self.cache.get_mut(op_code)
    }

}