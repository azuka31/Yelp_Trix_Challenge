create table df_business(
	address varchar,
	acceptsinsurance varchar,
	agesallowed varchar,
	alcohol varchar,
	ambience varchar,
	byob varchar,
	byobcorkage varchar,
	bestnights varchar,
	bikeparking varchar,
	businessacceptsbitcoin varchar,
	businessacceptscreditcards varchar,
	businessparking varchar,
	byappointmentonly varchar,
	caters varchar,
	coatcheck varchar,
	corkage varchar,
	dietaryrestrictions varchar,
	dogsallowed varchar,
	drivethru varchar,
	goodfordancing varchar,
	goodforkids varchar,
	goodformeal varchar,
	hairspecializesin varchar,
	happyhour varchar,
	hastv varchar,
	music varchar,
	noiselevel varchar,
	open24hours varchar,
	outdoorseating varchar,
	restaurantsattire varchar,
	restaurantscounterservice varchar,
	restaurantsdelivery varchar,
	restaurantsgoodforgroups varchar,
	restaurantspricerange2 varchar,
	restaurantsreservations varchar,
	restaurantstableservice varchar,
	restaurantstakeout varchar,
	smoking varchar,
	wheelchairaccessible varchar,
	wifi varchar,
	business_id varchar,
	categories varchar,
	city varchar,
	friday varchar,
	monday varchar,
	saturday varchar,
	sunday varchar,
	thursday varchar,
	tuesday varchar,
	wednesday varchar,
	is_open bigint,
	latitude float8,
	longitude float8,
	name varchar,
	postal_code varchar,
	review_count bigint,
	stars float8,
	state varchar
);

create table df_checkin(
	business_id varchar,
	date varchar
);

create table df_review(
	business_id varchar,
	cool bigint,
	date varchar,
	funny bigint,
	review_id varchar,
	stars float8,
	text varchar,
	useful bigint,
	user_id varchar
);

create table df_tip(
	business_id varchar,
	compliment_count bigint,
	date varchar,
	text varchar,
	user_id varchar
);

create table df_user(
	average_stars float8,
	compliment_cool bigint,
	compliment_cute bigint,
	compliment_funny bigint,
	compliment_hot bigint,
	compliment_list bigint,
	compliment_more bigint,
	compliment_note bigint,
	compliment_photos bigint,
	compliment_plain bigint,
	compliment_profile bigint,
	compliment_writer bigint,
	cool bigint,
	elite varchar,
	fans bigint,
	friends varchar,
	funny bigint,
	name varchar,
	review_count bigint,
	useful bigint,
	user_id varchar,
	yelping_since varchar
);

create table df_business_summary(
	business_id varchar,
	name varchar,
	stars float8,
	conclusion varchar
);

