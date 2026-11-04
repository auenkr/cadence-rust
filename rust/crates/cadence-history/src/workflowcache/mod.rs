//! Per-workflow external and internal rate limiter cache.
use std::{collections::HashMap,sync::{Arc,Mutex},time::{Duration,Instant}};
pub const ERR_DOMAIN_NAME:&str="failed to get domain name from domainID";
pub const ERR_CACHE_INSERT:&str="Failed to insert new value into cache";
/// Domain lookup errors intentionally fail open, matching Go WFCache.
pub trait DomainNames:Send+Sync{fn domain_name(&self,domain_id:&str)->Result<String,String>;}
pub trait Limiter:Send+Sync{fn allow(&self)->bool;}
pub trait LimiterFactory:Send+Sync{fn limiter(&self,domain_name:&str)->Arc<dyn Limiter>;}
#[derive(Debug,Clone,Copy,PartialEq,Eq)]pub enum RequestKind{External,Internal}
struct Item{external:Arc<dyn Limiter>,internal:Arc<dyn Limiter>,last_access:Instant,external_count:WorkflowIdCountMetric,internal_count:WorkflowIdCountMetric}
pub struct WorkflowCache{domains:Arc<dyn DomainNames>,external_factory:Arc<dyn LimiterFactory>,internal_factory:Arc<dyn LimiterFactory>,items:Mutex<HashMap<(String,String),Item>>,ttl:Duration,max_count:usize}
impl WorkflowCache{pub fn new(domains:Arc<dyn DomainNames>,external_factory:Arc<dyn LimiterFactory>,internal_factory:Arc<dyn LimiterFactory>,ttl:Duration,max_count:usize)->Self{Self{domains,external_factory,internal_factory,items:Mutex::new(HashMap::new()),ttl,max_count}}
 pub fn allow_external(&self,domain_id:&str,workflow_id:&str)->bool{self.allow(domain_id,workflow_id,RequestKind::External)} pub fn allow_internal(&self,domain_id:&str,workflow_id:&str)->bool{self.allow(domain_id,workflow_id,RequestKind::Internal)}
 pub fn allow(&self,domain_id:&str,workflow_id:&str,kind:RequestKind)->bool{let Ok(domain_name)=self.domains.domain_name(domain_id)else{return true};let key=(domain_name.clone(),workflow_id.to_owned());let now=Instant::now();let mut items=self.items.lock().unwrap();items.retain(|_,v|now.duration_since(v.last_access)<=self.ttl);if let Some(v)=items.get_mut(&key){v.last_access=now;return match kind{RequestKind::External=>{v.external_count.update(now);v.external.allow()},RequestKind::Internal=>{v.internal_count.update(now);v.internal.allow()}}}let mut value=Item{external:self.external_factory.limiter(&domain_name),internal:self.internal_factory.limiter(&domain_name),last_access:now,external_count:WorkflowIdCountMetric::default(),internal_count:WorkflowIdCountMetric::default()};if self.max_count>0&&items.len()>=self.max_count{if let Some(old)=items.iter().min_by_key(|(_,v)|v.last_access).map(|(k,_)|k.clone()){items.remove(&old);}}let allowed=match kind{RequestKind::External=>{value.external_count.update(now);value.external.allow()},RequestKind::Internal=>{value.internal_count.update(now);value.internal.allow()}};items.insert(key,value);allowed}
 pub fn size(&self)->usize{self.items.lock().unwrap().len()}
}
#[derive(Debug,Default)]pub struct WorkflowIdCountMetric{starting_second:Option<Instant>,count:i32}
impl WorkflowIdCountMetric{pub fn reset(&mut self){self.reset_at(Instant::now())}pub fn reset_at(&mut self,now:Instant){self.starting_second=Some(now);self.count=0}pub fn update(&mut self,now:Instant)->i32{if self.starting_second.is_none_or(|start|now.duration_since(start)>Duration::from_secs(1)){self.reset_at(now)}self.count+=1;self.count}pub fn count(&self)->i32{self.count}}
