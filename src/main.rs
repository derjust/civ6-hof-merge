use rusqlite::{params, Connection, Result, NO_PARAMS};
use serde::{Deserialize, Serialize};
use serde_rusqlite::*;
use std::fs::File;

use log::{debug, info};
use maplit::hashset;
use structopt::StructOpt;

#[derive(StructOpt)]
#[structopt(name = "civ6-hof-merge", about = "Merges two HallOfFame-SQLite database files into one")]
struct Cli {
    #[structopt(parse(from_os_str))]
    source1: std::path::PathBuf,
    #[structopt(parse(from_os_str))]
    source2: std::path::PathBuf,
    #[structopt(parse(from_os_str))]
    target: std::path::PathBuf,
}

type GameId = i64;

#[derive(Deserialize, Serialize, Debug)]
struct GameDataPointValue {
    #[serde(alias = "DataPoint")]
    data_point: String,
    #[serde(alias = "GameId")]
    game_id: GameId,
    #[serde(alias = "ValueObjectId")]
    value_object_id: Option<i32>,
    #[serde(alias = "ValueType")]
    value_type: Option<String>,
    #[serde(alias = "ValueString")]
    value_string: Option<String>,
    #[serde(alias = "ValueNumeric")]
    value_numeric: Option<i32>,
}

#[derive(Deserialize, Serialize, Debug)]
struct GamePlayer {
    #[serde(alias = "PlayerObjectId")]
    player_object_id: i32,
    #[serde(alias = "IsLocal")]
    is_local: bool,
    #[serde(alias = "IsAI")]
    is_ai: bool,
    #[serde(alias = "IsMajor")]
    is_major: bool,
    #[serde(alias = "LeaderType")]
    leader_type: String,
    #[serde(alias = "LeaderName")]
    leader_name: Option<String>,
    #[serde(alias = "CivilizationType")]
    civilization_type: Option<String>,
    #[serde(alias = "CivilizationName")]
    civilization_name: Option<String>,
    #[serde(alias = "DifficultyType")]
    difficulty_type: Option<String>,
    #[serde(alias = "Score")]
    score: i32,
    #[serde(alias = "PlayerId")]
    player_id: i32,
    #[serde(alias = "TeamId")]
    team_id: i32,
}

#[derive(Deserialize, Serialize, Debug)]
struct GameObject {
    #[serde(alias = "ObjectId")]
    object_id: i32,
    #[serde(alias = "GameId")]
    game_id: GameId,
    #[serde(alias = "PlayerObjectId")]
    player_object_id: Option<i32>,
    #[serde(alias = "Type")]
    _type: String,
    #[serde(alias = "Name")]
    name: Option<String>,
    #[serde(alias = "PlotIndex")]
    plot_index: Option<i32>,
    #[serde(alias = "ExtraData")]
    extra_data: Option<String>,
    #[serde(alias = "Icon")]
    icon: Option<String>,
}

#[derive(Deserialize, Serialize, Debug)]
struct Game {
    #[serde(alias = "GameId")]
    game_id: GameId,
    #[serde(alias = "Ruleset")]
    rule_set: String,
    #[serde(alias = "GameMode")]
    game_mode: i32,
    #[serde(alias = "TurnCount")]
    turn_count: i32,
    #[serde(alias = "GameSpeedType")]
    game_speed_type: String,
    #[serde(alias = "MapSizeType")]
    map_size_type: String,
    #[serde(alias = "Map")]
    map: String,
    #[serde(alias = "StartEraType")]
    start_era_type: String,
    #[serde(alias = "StartTurn")]
    start_turn: i32,
    #[serde(alias = "VictorTeamId")]
    victor_team_id: Option<i32>,
    #[serde(alias = "VictoryType")]
    victory_type: Option<String>,
    #[serde(alias = "LastPlayed")]
    last_played: i32,
}

fn open_db(path: &std::path::PathBuf) -> Result<Connection> {
    let con = Connection::open(path)?;

    let mut stmt = con.prepare("SELECT name FROM sqlite_master where type='table'")?;
    let tables = stmt.query_map(NO_PARAMS, |row| {
        let x: String = row.get(row.column_index("name")?)?;
        Ok(x)
    })?;

    let mut expected_tables = hashset! {"Migrations", "Rulesets", "RulesetTypes", "Games", "GamePlayers",
    "GameObjects", "RulesetDataPointValues", "GameDataPointValues", "ObjectDataPointValues", "DataSets", "DataSetValues"};

    for table in tables {
        let t: &str = &table?;
        expected_tables.remove(t);
        debug!("Found expected table {:?}", &t);
    }
    drop(stmt);

    if expected_tables.len() > 0 {
        panic!("Didn't find expected table(s) {:?}", expected_tables);
    }

    info!("Verification of {:?} successful", &path);
    Ok(con)
}

fn insert_game_if_not_exists(con: &Connection, game: &Game) -> Result<i64> {
    let mut stmt = con.prepare("INSERT INTO Games (Ruleset, GameMode, TurnCount, GameSpeedType, MapSizeType, Map, StartEraType, StartTurn, VictorTeamId, VictoryType, LastPlayed)\
    SELECT ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11\
    WHERE NOT EXISTS(SELECT 1 FROM Games WHERE Ruleset = ?1 AND GameMode = ?2 AND TurnCount = ?3 AND GameSpeedType = ?4 AND MapSizeType = ?5 AND Map = ?6 AND StartEraType = ?7 AND StartTurn = ?8 AND VictorTeamId = ?9 AND VictoryType = ?10 AND LastPlayed = ?11)")?;

    let params = params![
        game.rule_set,
        game.game_mode,
        game.turn_count,
        game.game_speed_type,
        game.map_size_type,
        game.map,
        game.start_era_type,
        game.start_turn,
        game.victor_team_id,
        game.victory_type,
        game.last_played,
    ];
    debug!("SQL: {:?}", stmt);
    let row_id = stmt.insert(params)?;
    debug!("{}", row_id);
    Ok(row_id)
}

fn copy_game_object(
    source_connection: &Connection,
    game_id: GameId,
    target_connection: &Connection,
    new_game_id: GameId,
    void: &i32,
) -> Result<i64, Box<dyn std::error::Error>> {
    debug!("Copying GameObject {} for game {}", &void, &game_id);

    let mut stmt = source_connection.prepare("SELECT ObjectId, GameId, PlayerObjectId, Type, Name, PlotIndex, ExtraData, Icon FROM GameObjects WHERE GameId = ? AND ObjectId = ?")?;

    let go = stmt
        .query_and_then(params![game_id, void], from_row::<GameObject>)?
        .next()
        .ok_or(rusqlite::Error::QueryReturnedNoRows)??;

    let mut stmt = target_connection.prepare("INSERT INTO GameObjects (GameId, PlayerObjectId, Type, Name, PlotIndex, ExtraData, Icon) VALUES (?, ?, ?, ?, ?, ?, ?)")?;

    let row_id = stmt.insert(params![
        new_game_id,
        go.player_object_id,
        go._type,
        go.name,
        go.plot_index,
        go.extra_data,
        go.icon,
    ])?;

    info!("Copied GameObject {} as {}", &void, &row_id);
    Ok(row_id)
}

fn copy_game_objects(
    source_connection: &Connection,
    game_id: GameId,
    target_connection: &Connection,
    new_game_id: GameId,
    exclude_object_ids: Vec<i32>,
) -> Result<i32, Box<dyn std::error::Error>> {
    debug!("Copying GameObjects for game {} skipping {:?}", &game_id, &exclude_object_ids);

    let mut stmt = source_connection.prepare("SELECT ObjectId, GameId, PlayerObjectId, Type, Name, PlotIndex, ExtraData, Icon FROM GameObjects WHERE GameId = ? AND ObjectId NOT IN (?)")?;
    let mut go_counter = 0;
    let excluded_ids: Vec<String> = exclude_object_ids.iter().map(|&x| x.to_string()).collect();
    let rows_iter = from_rows::<GameObject>(stmt.query(params![game_id, excluded_ids.join(",")])?);

    for game_object in rows_iter {
        go_counter += 1;
        let go = game_object?;

        let mut stmt = target_connection.prepare("INSERT INTO GameObjects (GameId, PlayerObjectId, Type, Name, PlotIndex, ExtraData, Icon) VALUES (?, ?, ?, ?, ?, ?, ?)")?;

        let goid: Option<i64>;
        if go.player_object_id.is_some() {
            goid = Some(copy_game_players(
                &source_connection,
                go.player_object_id.unwrap(),
                &target_connection,
            )?);
        } else {
            goid = None;
        }

        let row_id = stmt.insert(params![
            new_game_id,
            goid,
            go._type,
            go.name,
            go.plot_index,
            go.extra_data,
            go.icon,
        ])?;

        debug!("Inserted GameObject {:?} under {}", &go, &row_id);
    }

    info!(
        "Copied {} GameObjects from game {} to {}",
        &go_counter, &game_id, &new_game_id
    );
    Ok(go_counter)
}

fn copy_game_players(
    source_connection: &Connection,
    player_object_id: i32,
    target_connection: &Connection,
) -> std::result::Result<i64, Box<dyn std::error::Error>> {
    debug!("Copying GamePlayer {}", &player_object_id);
    let mut stmt = source_connection.prepare("SELECT PlayerObjectId,IsLocal,IsAI,IsMajor,LeaderType,LeaderName,CivilizationType,CivilizationName,DifficultyType,Score,PlayerId,TeamId FROM GamePlayers WHERE PlayerObjectId = ?")?;

    let gp = stmt
        .query_and_then(params![player_object_id], from_row::<GamePlayer>)?
        .next()
        .ok_or(rusqlite::Error::QueryReturnedNoRows)??;

    let mut stmt = target_connection.prepare("INSERT INTO GamePlayers (IsLocal,IsAI,IsMajor,LeaderType,LeaderName,CivilizationType,CivilizationName,DifficultyType,Score,PlayerId,TeamId) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")?;

    let row_id = stmt.insert(params![
        gp.is_local,
        gp.is_ai,
        gp.is_major,
        gp.leader_type,
        gp.leader_name,
        gp.civilization_type,
        gp.civilization_name,
        gp.difficulty_type,
        gp.score,
        gp.player_id,
        gp.team_id,
    ])?;

    info!("Copied GamePlayers {} as {}", &player_object_id, &row_id);
    Ok(row_id)
}

fn copy_game_data_point_value(
    source_connection: &Connection,
    game_id: GameId,
    target_connection: &Connection,
    new_game_id: GameId,
) -> std::result::Result<Vec<i32>, Box<dyn std::error::Error>> {
    debug!("Copying GameDataPointValue for game {}", &game_id);
    let mut already_copied_game_objects = Vec::new();
    let mut stmt = source_connection.prepare("SELECT DataPoint, GameId, ValueObjectId, ValueType, ValueString, ValueNumeric FROM GameDataPointValues WHERE GameId = ?")?;

    let mut gdpv_counter = 0;
    let rows_iter = from_rows::<GameDataPointValue>(stmt.query(params![game_id])?);

    for game_data_point_value in rows_iter {
        gdpv_counter += 1;
        let gdpv = game_data_point_value?;

        let mut stmt = target_connection.prepare("INSERT INTO GameDataPointValues (DataPoint, GameId, ValueObjectId, ValueType, ValueString, ValueNumeric) VALUES (?, ?, ?, ?, ?, ?)")?;

        let new_value_object_id;
        if gdpv.value_object_id.is_some() {
            let voi = gdpv.value_object_id.unwrap();
            new_value_object_id = Some(copy_game_object(source_connection, game_id, target_connection, new_game_id, &voi)?);
            already_copied_game_objects.push(voi);
        } else {
            new_value_object_id = None;
        }

        let row_id = stmt.insert(params![
            gdpv.data_point,
            new_game_id,
            new_value_object_id,
            gdpv.value_type,
            gdpv.value_string,
            gdpv.value_numeric,
        ])?;

        debug!("Inserted GameDataPointValue {:?} under {}", &gdpv, &row_id);
    }

    info!(
        "Copied {} GameDataPointValue from game {} to {}",
        &gdpv_counter, &game_id, &new_game_id
    );
    Ok(already_copied_game_objects)
// )
}

fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let args = Cli::from_args();

    //TODO use path
    let source_path = args.source1;//"/Users/sebastian/Library/Application Support/Sid Meier's Civilization VI/HallofFame.sqlite";
    let enrich_path = args.source2;//"HallofFame.sqlite";
    let target_path = args.target;//"target.sqlite";

    let mut source_file = File::open(&source_path)?;
    let mut target_file = File::create(&target_path)?;
    let copy_bytes = std::io::copy(&mut source_file, &mut target_file)?;

    info!(
        "Created {:?} with {}b based of {:?}",
        &target_path, copy_bytes, &source_file
    );

    let source_connection2 = open_db(&enrich_path)?;

    let target_connection = Connection::open(&target_path)?;

    let mut stmt = source_connection2.prepare("SELECT * FROM Games")?;
    let rows_iter = from_rows::<Game>(stmt.query(NO_PARAMS)?);

    info!("Synchronizing games:");
    for game in rows_iter {
        //debug!("Loaded: {:?}", &game);

        let g = &game?;

        let row_id = insert_game_if_not_exists(&target_connection, &g)?;

        if row_id == 0 {
            info!("-")
        } else {
            let copied_game_data_point_values = copy_game_data_point_value(&source_connection2, g.game_id, &target_connection, row_id)?;
            copy_game_objects(&source_connection2, g.game_id, &target_connection, row_id, copied_game_data_point_values)?;
            info!("Copied game {} to {}", &g.game_id, &row_id);
        }
    }

    Ok(())
}
