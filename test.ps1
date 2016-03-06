Invoke-WebRequest -Uri http://localhost:8080/test/item1 -Body "Value1" -Method Post
Invoke-WebRequest -Uri http://localhost:8080/test/item2 -Body "Value2" -Method Post
Invoke-WebRequest -Uri http://localhost:8080/test/item3 -Body "Value3" -Method Post
Invoke-WebRequest -Uri http://localhost:8080/test/item4 -Body "Value4" -Method Post


Invoke-WebRequest -Uri http://localhost:8080/test/item1 -Method Get
Invoke-WebRequest -Uri http://localhost:8080/test/item2 -Method Get
Invoke-WebRequest -Uri http://localhost:8080/test/item3 -Method Get
Invoke-WebRequest -Uri http://localhost:8080/test/item4 -Method Get


Invoke-WebRequest -Uri http://localhost:8080/test/item1 -Body "Ve1" -Method Put
Invoke-WebRequest -Uri http://localhost:8080/test/item2 -Body "Ve2" -Method Put
Invoke-WebRequest -Uri http://localhost:8080/test/item3 -Body "Ve3" -Method Put
Invoke-WebRequest -Uri http://localhost:8080/test/item4 -Body "Ve4" -Method Put

Invoke-WebRequest -Uri http://localhost:8080/test/item1 -Method Get
Invoke-WebRequest -Uri http://localhost:8080/test/item2 -Method Get
Invoke-WebRequest -Uri http://localhost:8080/test/item3 -Method Get
Invoke-WebRequest -Uri http://localhost:8080/test/item4 -Method Get

Invoke-WebRequest -Uri http://localhost:8080/test/item1 -Method Delete
Invoke-WebRequest -Uri http://localhost:8080/test/item2 -Method Delete
Invoke-WebRequest -Uri http://localhost:8080/test/item3 -Method Delete
Invoke-WebRequest -Uri http://localhost:8080/test/item4 -Method Delete

Invoke-WebRequest -Uri http://localhost:8080/test/item1 -Method Get
Invoke-WebRequest -Uri http://localhost:8080/test/item2 -Method Get
Invoke-WebRequest -Uri http://localhost:8080/test/item3 -Method Get
Invoke-WebRequest -Uri http://localhost:8080/test/item4 -Method Get

