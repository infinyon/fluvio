//!
//! # Auth Token Actions
//!
//! Converts Kubernetes Auth-Token events into Auth-Token actions
//!
use std::fmt::Debug;
use std::fmt::Display;

use log::{error, trace};
use log::warn;
use log::debug;
use utils::actions::Actions;
use k8_metadata::core::metadata::K8List;
use k8_metadata::core::metadata::K8Obj;
use k8_metadata::core::metadata::K8Watch;
use k8_metadata::core::Spec as K8Spec;
use k8_metadata::client::TokenStreamResult;
use k8_metadata::client::MetadataClientError;


use crate::core::common::KVObject;
use crate::core::common::LSChange;
use crate::core::common::LocalStore;
use crate::core::Spec;
use crate::core::Status;
use crate::ScServerError;

///
/// Translate incoming k8 items into KVInputAction against MemStore which contains local state
/// It only generates KVInputAction if incoming k8 object is different from memstore
/// 
///
pub fn k8_events_to_metadata_actions<S>(
    k8_tokens: K8List<S::K8Spec,<S::K8Spec as K8Spec>::Status>,
    local_store: &LocalStore<S>,
) -> Actions<LSChange<S>> 
    where 
        S: Spec + PartialEq + Debug, 
        S::Status: Status + PartialEq + Debug , 
        S::K8Spec: Debug, 
        S::Key: Clone + Ord  + Debug + Display
{
    let (mut add_cnt, mut mod_cnt, mut del_cnt, mut skip_cnt) = (0, 0, 0, 0);
    let mut local_names = local_store.all_keys();
    let all = local_store.count();
    let mut actions: Actions<LSChange<S>> = Actions::default();

    // loop through items and generate add/mod actions
    for k8_obj in k8_tokens.items {

        match k8_obj_to_kv_obj(k8_obj) {

            Ok(new_kv_value) =>  {
                let key = new_kv_value.key_owned();
                if let Some(old_value) = local_store.value(&key) {
                    // object exists
                    if old_value == new_kv_value {
                        skip_cnt += 1; //nothing changed
                    } else {
                        // diff
                        mod_cnt += 1;
                        debug!("adding {}:{} to local store",S::LABEL,new_kv_value.key());
                        local_store.insert(new_kv_value.clone());
                        actions.push(LSChange::update(new_kv_value, old_value));
                    }
                    
                    local_names.retain(|n| *n != key);
                } else {
                    // object doesn't exisit
                    add_cnt += 1;
                    local_store.insert(new_kv_value.clone());
                    actions.push(LSChange::add(new_kv_value));
                }
            },
            Err(err) => {
                error!("{}", err);
                skip_cnt += 1;
            }
        }

    }

    // loop through the remaining names and generate delete actions
    for name in local_names.into_iter() {
        if local_store.contains_key(&name) {
          
            if let Some(old_value) = local_store.remove(&name) {
                del_cnt += 1;
                actions.push(LSChange::delete(old_value));
            } else {
                skip_cnt += 1;
                error!("delete  should never fail since key exists: {:#?}",name);
            }
           
        } else {
            skip_cnt += 1;
            error!("kv unexpectedly removed... skipped {:#?}", name);
        }
    }

    // log counters
    trace!(
        "KV {} events => local: {} [add:{}, mod:{}, del:{}, skip:{}]",
        S::LABEL,
        all,
        add_cnt,
        mod_cnt,
        del_cnt,
        skip_cnt
    );

    actions
}

///
/// Translates K8 events into metadata action.
///
pub fn k8_event_stream_to_metadata_actions<S,E>(
    stream: TokenStreamResult<S::K8Spec,<S::K8Spec as K8Spec>::Status,E>,
    local_store: &LocalStore<S>
) -> Actions<LSChange<S>> 
    where 
        S: Spec + Debug + PartialEq + Debug,
        <S as Spec>::K8Spec: Debug,
        S::Key: Debug + Display + Clone,
        S::Status: Debug + PartialEq,
        E: MetadataClientError
{

    let (mut add_cnt, mut mod_cnt, mut del_cnt, mut skip_cnt) = (0, 0, 0, 0);
    let mut actions: Actions<LSChange<S>> = Actions::default();

    // loop through items and generate add/mod actions
    for token in stream.unwrap() {
        match token {
            Ok(watch_obj) => match watch_obj {
                K8Watch::ADDED(k8_obj) => {
                    let converted: Result<KVObject<S>,ScServerError> = k8_obj_to_kv_obj(k8_obj);  // help out compiler
                    match converted {
                        Ok(new_kv_value) =>  {
                            trace!("KV ({}): push ADD action", new_kv_value.key());
                            if let Some(old_value) = local_store.insert(new_kv_value.clone()) {
                                // some old value, check if same as new one, if they are same, no need for action
                                 warn!("detected exist value: {:#?} which sould not exists",old_value);
                                if old_value == new_kv_value {
                                    trace!("same value as old value, ignoring");
                                } else {
                                    trace!("generating update action: {:#?}",new_kv_value.key());
                                    actions.push(LSChange::update(new_kv_value,old_value));
                                    mod_cnt += 1;
                                }
                            } else {
                                // no existing value, which should be expected
                                debug!("adding {}:{} to local store",S::LABEL,new_kv_value.key());
                                actions.push(LSChange::add(new_kv_value));
                                
                                add_cnt += 1;
                            }
                        },
                        Err(err) => {
                            error!("{}", err);
                            skip_cnt += 1;
                        }
                    }
                },
                K8Watch::MODIFIED(k8_obj) => {
                    let converted: Result<KVObject<S>,ScServerError> = k8_obj_to_kv_obj(k8_obj);  // help out compiler
                    match converted {
                        Ok(new_kv_value) =>  {

                            if let Some(old_value) = local_store.insert(new_kv_value.clone()) {

                                if old_value == new_kv_value {
                                    // this is unexpected, 
                                    warn!("old and new value is same: {:#?}, ignoring",new_kv_value);
                                } else {
                                    // normal
                                    actions.push(LSChange::update(new_kv_value, old_value));
                                    mod_cnt += 1;
                                }
                            } else {
                                // doesn't exist, this is then new
                                warn!("KV ({}) - not found, generating add", new_kv_value.key());
                                actions.push(LSChange::add(new_kv_value));
                            }
                        },
                        Err(err) => {
                            error!("{}", err);
                            skip_cnt += 1;
                        }
                    }
                    
                }
                K8Watch::DELETED(k8_obj) => {
                    match k8_obj_to_kv_obj(k8_obj) {
                        Ok(kv_value) =>  {
                            trace!("KV ({}): push DEL action", kv_value.key());

                            // try to delete it
                            if let Some(_old_value) = local_store.remove(kv_value.key()) {
                                del_cnt += 1;
                                actions.push(LSChange::delete(kv_value));
                            } else {
                                skip_cnt += 1;
                                warn!("delete  should never fail since key exists: {}",kv_value.key());
                            }
                        },
                        Err(err) => {
                            error!("{}", err);
                            skip_cnt += 1;
                        }
                    }
                }
            },
            Err(err) => error!("Problem parsing {} event: {} ... (exiting)", S::LABEL,err),
        }
        
    }

    // log counters
    let all = add_cnt + mod_cnt + del_cnt + skip_cnt;
    trace!("K8 Streams {} [all:{}, add:{},mod:{},del:{},ski: {}", 
        S::LABEL,
        all, 
        add_cnt, 
        mod_cnt, 
        del_cnt, 
        skip_cnt);

    actions
}

///
/// Translates K8 object into Sc AuthToken metadata
///
fn k8_obj_to_kv_obj<S>(k8_obj: K8Obj<S::K8Spec,<S::K8Spec as K8Spec>::Status>) -> Result<KVObject<S>,ScServerError> 
     where 
        S: Spec + Debug,
         <S as Spec>::K8Spec: Debug

{
    trace!("converting k8: {:#?}",k8_obj.spec);
    S::convert_from_k8(k8_obj)
        .map(|val| {
            trace!("converted val: {:#?}",val.spec);
            val
        })
        .map_err(|err| err.into())
}



#[cfg(test)]
pub mod test {
   
    use k8_metadata::topic::TopicSpec as K8TopicSpec;
    use k8_metadata::topic::TopicStatus as K8TopicStatus;
    use k8_metadata::topic::TopicStatusResolution as K8topicStatusResolution;
    use k8_metadata::core::metadata::K8List;
    use k8_metadata::core::metadata::K8Obj;
    use k8_metadata::core::metadata::K8Watch;
    use k8_metadata::client::as_token_stream_result;
    use k8_metadata::client::DoNothingError;
    
    //use k8_metadata::core::metadata::K8Watch;
    //use k8_metadata::core::Spec as K8Spec;
    use crate::core::common::LSChange;
    use crate::core::topics::TopicLocalStore;

    use super::k8_events_to_metadata_actions;
    use super::k8_event_stream_to_metadata_actions;
    use super::k8_obj_to_kv_obj;
   

    type TopicList = K8List<K8TopicSpec,K8TopicStatus>;
    type K8Topic = K8Obj<K8TopicSpec,K8TopicStatus>;
    type K8TopicWatch = K8Watch<K8TopicSpec,K8TopicStatus>;



    #[test]
    fn test_check_items_against_empty() {
 
        let mut topics = TopicList::new();
        topics.items.push(K8Topic::new("topic1",K8TopicSpec::default()));
    
        let topic_store = TopicLocalStore::default();

        let kv_actions = k8_events_to_metadata_actions(topics,&topic_store);

        assert_eq!(kv_actions.count(),1);
        let action = kv_actions.iter().next().expect("first");
        match action {
            LSChange::Add(new_value) => {
                assert_eq!(new_value.key(),"topic1");
            }
            _ => assert!(false),
        }
        topic_store.value(&"topic1".to_owned()).expect("topic1 should exists");

    }

    #[test]
    fn test_check_items_against_same() {
 
        let mut topics = TopicList::new();
        topics.items.push(K8Topic::new("topic1",K8TopicSpec::default()));
    
        let topic_store = TopicLocalStore::default();
        let topic_kv = k8_obj_to_kv_obj(K8Topic::new("topic1",K8TopicSpec::default())).expect("work");
        topic_store.insert(topic_kv);

        let kv_actions = k8_events_to_metadata_actions(topics,&topic_store);

        assert_eq!(kv_actions.count(),0);
    }

    #[test]
    fn test_items_generate_modify() {

        let mut status = K8TopicStatus::default();
        status.resolution = K8topicStatusResolution::Provisioned;
        let new_topic =  K8Topic::new("topic1",K8TopicSpec::default())
                .set_status(status);
        let old_topic = K8Topic::new("topic1",K8TopicSpec::default());

        let mut topics = TopicList::new();
        topics.items.push(new_topic.clone());
    
        let topic_store = TopicLocalStore::default();
        let old_kv = k8_obj_to_kv_obj(old_topic).expect("conversion");
        topic_store.insert(old_kv.clone());

        let kv_actions = k8_events_to_metadata_actions(topics,&topic_store);

        assert_eq!(kv_actions.count(),1);
        let action = kv_actions.iter().next().expect("first");
        match action {
            LSChange::Mod(new,old) => {
                let new_kv = k8_obj_to_kv_obj(new_topic).expect("conversion");
                assert_eq!(new.key(),new_kv.key());
                assert_eq!(new,&new_kv);
                assert_eq!(old,&old_kv);
            }
            _ => assert!(false),
        }
    }

    #[test]
    fn test_items_delete() {
 
        let topics = TopicList::new();
        
        let topic_store = TopicLocalStore::default();
        let topic_kv = k8_obj_to_kv_obj(K8Topic::new("topic1",K8TopicSpec::default())).expect("work");
        topic_store.insert(topic_kv);

        let kv_actions = k8_events_to_metadata_actions(topics,&topic_store);

        assert_eq!(kv_actions.count(),1);
        let action = kv_actions.iter().next().expect("first");
        match action {
            LSChange::Delete(old_value) => {
                assert_eq!(old_value.key(),"topic1");
            }
            _ => assert!(false),
        }
    }




    #[test]
    fn test_watch_add_actions() {
 
        let new_topic =  K8Topic::new("topic1",K8TopicSpec::default())
                .set_status(K8TopicStatus::default());
       

        let mut watches = vec![];
        watches.push(K8TopicWatch::ADDED(new_topic.clone()));
      
    
        let topic_store = TopicLocalStore::default();

        let kv_actions = k8_event_stream_to_metadata_actions::<_,DoNothingError>(as_token_stream_result(watches),&topic_store);

        assert_eq!(kv_actions.count(),1);
        let action = kv_actions.iter().next().expect("first");
        match action {
            LSChange::Add(new_value) => {
                assert_eq!(new_value.key(),"topic1");
            }
            _ => assert!(false),
        }
        topic_store.value(&"topic1".to_owned()).expect("topic1 should exists");

    }


}
       