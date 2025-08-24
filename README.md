## How to Run

1. Clone the repository  
2. Run `npm install` to install dependencies  
3. `cd` into the repository  
4. Start the server with `./your_program.sh`  
5. Connect to the server using a [Redis client](https://redis.io/docs/latest/develop/connect/clients/), for example `redis-cli`  
6. Run commands like `PING`, `ECHO`, `GET`, `SET`, etc.

### Example

```bash
redis-cli SET name john
redis-cli GET name