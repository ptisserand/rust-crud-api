use actix_web::middleware::Logger;
use actix_web::{delete, get, post, put, web, App, HttpResponse, HttpServer, Responder, Result};
use env_logger::Env;
use log::info;
use std::sync::{Arc, Mutex};

use tokio_postgres::{Client, NoTls};

#[macro_use]
extern crate serde_derive;

// Mode: User struct with id, name, email
#[derive(Serialize, Deserialize)]
struct User {
    id: Option<i32>,
    name: String,
    email: String,
}

// DATABASE URL
const DB_URL: &str = env!("DATABASE_URL");

// CONTROLLERS
#[get("/users")]
async fn get_users(db: web::Data<Arc<Mutex<Client>>>) -> impl Responder {
    info!("Retrieving list of users");
    let client = db.lock().unwrap();
    let mut users = Vec::new();
    for row in client.query("SELECT * from users", &[]).await.unwrap() {
        users.push(User {
            id: row.get(0),
            name: row.get(1),
            email: row.get(2),
        });
    }

    HttpResponse::Ok().json(users)
}

#[post("/users")]
async fn create_user(body: web::Json<User>, db: web::Data<Arc<Mutex<Client>>>) -> impl Responder {
    info!("Create an user");
    let client = db.lock().unwrap();
    let user = body.into_inner();
    let result = client
        .query_one(
            "INSERT INTO users (name, email) VALUES ($1, $2) RETURNING id",
            &[&user.name, &user.email],
        )
        .await;
    if result.is_ok() {
        let id: i32 = result.unwrap().get(0);
        info!("New id: {}", id);
        let user = User {
            id: Some(id),
            name: user.name,
            email: user.email,
        };
        HttpResponse::Created().json(user)
    } else {
        HttpResponse::InternalServerError().body("Failed to insert into DB")
    }
}

#[get("/users/{id}")]
async fn get_user(path: web::Path<String>, db: web::Data<Arc<Mutex<Client>>>) -> impl Responder {
    let client = db.lock().unwrap();
    let path = path.into_inner();
    let id = path.parse::<i32>();
    if id.is_err() {
        return HttpResponse::InternalServerError().body(format!("Can't parse {} as an id", path));
    }
    let id = id.unwrap();
    info!("Retrieving user '{}'", id);

    match client
        .query_one("SELECT * FROM users WHERE id = $1", &[&id])
        .await
    {
        Ok(row) => {
            let user = User {
                id: row.get(0),
                name: row.get(1),
                email: row.get(2),
            };
            HttpResponse::Ok().json(user)
        }
        _ => {
            info!("User {} not found", id);
            HttpResponse::NotFound().body(format!("User {} not found", id))
        }
    }
}

#[put("/users/{id}")]
async fn update_user(
    path: web::Path<String>,
    body: web::Json<User>,
    db: web::Data<Arc<Mutex<Client>>>,
) -> impl Responder {
    let client = db.lock().unwrap();
    let path = path.into_inner();
    let mut user = body.into_inner();
    let id = path.parse::<i32>();
    if id.is_err() {
        return HttpResponse::InternalServerError().body(format!("Can't parse {} as an id", path));
    }
    let id = id.unwrap();
    let result = client
        .execute(
            "UPDATE users SET name = $1, email = $2 WHERE id = $3",
            &[&user.name, &user.email, &id],
        )
        .await;
    if result.is_ok() {
        let result = result.unwrap();
        if result != 0 {
            user.id = Some(id);
            HttpResponse::Ok().json(user)
        } else {
            HttpResponse::NotFound().finish()
        }
    } else {
        return HttpResponse::InternalServerError().body(format!("Failed to update user {}", id));
    }
}

#[delete("/users/{id}")]
async fn delete_user(path: web::Path<String>, db: web::Data<Arc<Mutex<Client>>>) -> impl Responder {
    let client = db.lock().unwrap();
    let path = path.into_inner();
    let id = path.parse::<i32>();
    if id.is_err() {
        return HttpResponse::InternalServerError().body(format!("Can't parse {} as an id", path));
    }
    let id = id.unwrap();
    info!("Deleting user '{}'", id);
    let rows_affected = client
        .execute("DELETE FROM users WHERE id = $1", &[&id])
        .await;
    if rows_affected.is_ok() {
        let rows_affected = rows_affected.unwrap();
        if rows_affected == 0 {
            HttpResponse::NotFound().body(format!("User {} not found", id))
        } else {
            HttpResponse::NoContent().finish()
        }
    } else {
        HttpResponse::InternalServerError().body("SQL query failed")
    }
}

// main function
#[actix_web::main]
async fn main() -> Result<(), std::io::Error> {
    // Initialize the logger
    env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .format_timestamp(None)
        .format_module_path(false)
        .init();

    info!("Setup database");
    // set database
    let db_client = Arc::new(Mutex::new(
        setup_database().await.expect("Failed to connect to DB"),
    ));
    HttpServer::new(move || {
        App::new()
            .wrap(Logger::default())
            .app_data(web::Data::new(db_client.clone()))
            .service(get_users)
            .service(create_user)
            .service(get_user)
            .service(update_user)
            .service(delete_user)
    })
    .bind(("0.0.0.0", 8080))?
    .run()
    .await
}

async fn setup_database() -> Result<tokio_postgres::Client, tokio_postgres::Error> {
    // connect to database
    let (client, connection) = tokio_postgres::connect(DB_URL, NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });

    // Create table
    client
        .batch_execute(
            "CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            name VARCHAR NOT NULL,
            email VARCHAR NOT NULL
        )",
        )
        .await?;
    Ok(client)
}
