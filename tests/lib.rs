use std::error::Error;
use redis_cmd::config::load_redis_config;

#[tokio::test]
async fn test() -> Result<(), Box<dyn Error>> {
    let c = load_redis_config("test_config.toml");
    let client = c.load_redis_pool().await?;
    let v =  client.clone().set("321", "123").await.unwrap();
    println!("{}", v);
    let result  = client.clone().get("321").await?;
    println!("{}", result);
    let ok = client.clone().exists("321").await.unwrap();
    println!("exists: {}", ok);
    let expire = client.clone().expire("321", 100).await?;
    println!("expire: {}", expire);

    let data = vec![("123", "321")];
    let ok = client.clone().xadd("rrrrr", &data.clone()).await?;
    println!("xadd: {}", ok);
    let v = client.clone().xgroup_create("rrrrr", "1234").await?;
    println!("xgroup_create: {:?}", v);

    // // let data = client.xinfo_groups("rrrrrr").await.unwrap();
    // // println!("xinfo_groups: {:?}", data);
    // let v =  client.clone().xread_group("rrrrr", "123", "32").await?;
    // println!("{:?}", v);
    // let data = client.clone().xclaim_auto("rrrrr", "123", "32", 60000).await.unwrap();
    // println!("xclaim_auto{:?}", data);
    // let data:String = client.get_id(&data).await.unwrap();
    // let data = client.clone().xack("rrrrr", "123", &vec![data] ).await.unwrap();
    // println!("xclaim_auto{:?}", data);
    // let data = client.clone().xpending_one("rrrrr", "123").await.unwrap();
    // let data:Vec<Vec<Value>> = from_redis_value(&data).unwrap();
    // let data = data.get(0).unwrap().get(0).unwrap();
    // let data:String = from_redis_value(data).unwrap();
    // println!("{:?}",  data);
    // assert_eq!(result, "123");
    Ok(())
}