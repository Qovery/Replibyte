db.createUser({
    user: 'root',
    pwd: 'password',
    roles: [
        {
            role: 'readWrite',
            db: 'test',
        },
    ],
});

db = new Mongo().getDB("test");

db.createCollection('users', { capped: false });
db.createCollection('states', { capped: false });
db.createCollection('cars', { capped: false });

for (let i = 0; i < 10; i++) {
    db.users.insertOne({
        name: 'user' + i,
        age: i,
    });
    db.states.insertOne({
        name: 'state' + i,
        number: i,
    });
    db.cars.insertOne({
        model: 'car' + i,
        year: 2010 + i,
    });
}