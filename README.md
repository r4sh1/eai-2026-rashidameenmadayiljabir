Enterprise Application Integration — 2026

Student: Rashid ameen madayil jabir
Repository Structure

    s0/ — Session 0
    pa1/ — PA1: Legacy File Ingestion
    pa2/ — PA2: RabbitMQ Publish and Consume
    pa3/ — PA3
    pa4/ — PA4
    pa5/ — PA5
    pa6/ — PA6
    pa7/ — PA7
    capstone/ — PA8 / final integration

Running PA1

cd pa1/starter
npm install
npm test
npm run typecheck

Running PA2

cd pa2
docker compose up -d --wait
cd starter
npm ci
npm test
npm run typecheck
cd ..
docker compose down -v
