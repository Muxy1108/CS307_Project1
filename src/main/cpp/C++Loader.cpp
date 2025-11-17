#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <thread>
#include <mutex>
#include <chrono>
#include <queue>
#include <memory>
#include <condition_variable>
#include <libpq-fe.h>
using namespace std;

const int THREAD_COUNT = 6;
const int BATCH_SIZE = 1000;
const int CONNECTION_POOL_SIZE = 8;

mutex coutMutex;


class ConnectionPool {
private:
    queue<PGconn*> connections;
    mutex poolMutex;
    condition_variable condition;
    string connectionString;
    int maxSize;

public:
    ConnectionPool(const string& connStr, int poolSize = CONNECTION_POOL_SIZE)
        : connectionString(connStr), maxSize(poolSize) {
        for (int i = 0; i < poolSize; ++i) {
            PGconn* conn = PQconnectdb(connStr.c_str());
            if (PQstatus(conn) == CONNECTION_OK) {
                connections.push(conn);
            } else {
                lock_guard<mutex> lock(coutMutex);

                PQfinish(conn);
            }
        }

        lock_guard<mutex> lock(coutMutex);
    }

    ~ConnectionPool() {
        lock_guard<mutex> lock(poolMutex);
        while (!connections.empty()) {
            PGconn* conn = connections.front();
            PQfinish(conn);
            connections.pop();
        }
    }

    PGconn* getConnection() {
        unique_lock<mutex> lock(poolMutex);

        condition.wait(lock, [this]() { return !connections.empty(); });

        PGconn* conn = connections.front();
        connections.pop();

        if (PQstatus(conn) != CONNECTION_OK) {
            PQreset(conn);
            if (PQstatus(conn) != CONNECTION_OK) {
                PQfinish(conn);
                conn = PQconnectdb(connectionString.c_str());
            }
        }

        return conn;
    }

    void returnConnection(PGconn* conn) {
        lock_guard<mutex> lock(poolMutex);


        if (PQstatus(conn) == CONNECTION_OK) {
            connections.push(conn);
        } else {
            PQfinish(conn);
            PGconn* newConn = PQconnectdb(connectionString.c_str());
            if (PQstatus(newConn) == CONNECTION_OK) {
                connections.push(newConn);
            }
        }

        condition.notify_one();
    }

    int availableConnections() {
        lock_guard<mutex> lock(poolMutex);
        return connections.size();
    }
};


unique_ptr<ConnectionPool> connectionPool;


string escapeSQL(const string& input) {
    string result;
    for (char c : input) {
        if (c == '\'') {
            result += "''";
        } else if (c == '\\') {
            result += "\\\\";
        } else {
            result += c;
        }
    }
    return result;
}

vector<string> splitCSV(const string& line) {
    vector<string> result;
    string current;
    bool inQuote = false;

    for (char c : line) {
        if (c == '"') {
            inQuote = !inQuote;
        } else if (c == ',' && !inQuote) {
            result.push_back(current);
            current.clear();
        } else {
            current.push_back(c);
        }
    }
    result.push_back(current);

    return result;
}

string buildInsertSQL(const vector<vector<string>>& rows) {
    string sql = "INSERT INTO users (author_id, author_name, gender, age, followers_count, following_count, user_followers, user_following) VALUES ";
    bool first = true;

    for (auto& r : rows) {
        if (!first) sql += ",";
        first = false;

        sql += "(";
        sql += r[0] + ", ";                          // author_id
        sql += "'" + escapeSQL(r[1]) + "', ";        // author_name
        sql += "'" + escapeSQL(r[2]) + "', ";        // gender
        sql += r[3] + ", ";                          // age
        sql += r[4] + ", ";                          // followers_count
        sql += r[5] + ", ";                          // following_count
        sql += "'" + escapeSQL(r[6]) + "', ";        // user_followers
        sql += "'" + escapeSQL(r[7]) + "'";          // user_following
        sql += ")";
    }

    sql += " ON CONFLICT (author_id) DO NOTHING;";
    return sql;
}

void importWorker(int id, vector<vector<string>> batch) {
    PGconn* conn = connectionPool->getConnection();

    PGresult* beginRes = PQexec(conn, "BEGIN;");
    if (PQresultStatus(beginRes) != PGRES_COMMAND_OK) {
        lock_guard<mutex> lock(coutMutex);
        cout << "thread " << id << " BEGIN failed: " << PQerrorMessage(conn) << endl;
        PQclear(beginRes);
        connectionPool->returnConnection(conn);
        return;
    }
    PQclear(beginRes);

    string sql = buildInsertSQL(batch);
    PGresult* res = PQexec(conn, sql.c_str());

    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        lock_guard<mutex> lock(coutMutex);
        cout << "thread " << id << " SQL error: " << PQerrorMessage(conn) << endl;

        PGresult* rollbackRes = PQexec(conn, "ROLLBACK;");
        PQclear(rollbackRes);
    } else {
        PGresult* commitRes = PQexec(conn, "COMMIT;");
        if (PQresultStatus(commitRes) != PGRES_COMMAND_OK) {
            lock_guard<mutex> lock(coutMutex);
            cout << "thread " << id << " COMMIT failed: " << PQerrorMessage(conn) << endl;
        } else {
            lock_guard<mutex> lock(coutMutex);
            cout << "thread " << id << " inserted " << batch.size() << " rows\n";
        }
        PQclear(commitRes);
    }
    PQclear(res);
    connectionPool->returnConnection(conn);
}

int main() {
#ifdef _WIN32
    system("chcp 65001 > nul");
#endif

    string connStr = "host=localhost port=5432 dbname=postgres user=postgres password=Xieyan2005";
    connectionPool = make_unique<ConnectionPool>(connStr);

    ifstream fin("data/user.csv");
    if (!fin.is_open()) {
        cout << "cannot open users.csv\n";
        return 0;
    }

    vector<vector<string>> rows;
    string line;

    getline(fin, line);

    while (getline(fin, line)) {
        auto cols = splitCSV(line);
        if (cols.size() >= 8) {
            rows.push_back(cols);
        } else {
            lock_guard<mutex> lock(coutMutex);
        }
    }
    fin.close();

    cout << "CSV loaded, total rows = " << rows.size() << endl;

    auto start_time = chrono::steady_clock::now();

    vector<thread> pool;
    int threadId = 0;
    int totalProcessed = 0;

    for (int i = 0; i < rows.size(); i += BATCH_SIZE) {
        int endIndex = min(i + BATCH_SIZE, (int)rows.size());
        vector<vector<string>> batch(rows.begin() + i, rows.begin() + endIndex);

        totalProcessed += batch.size();

        pool.emplace_back(importWorker, threadId++, batch);

        if (i % (BATCH_SIZE * 1000) == 0) {
            lock_guard<mutex> lock(coutMutex);
        }

        if (pool.size() == THREAD_COUNT) {
            for (auto& t : pool) t.join();
            pool.clear();
        }
    }

    for (auto& t : pool) t.join();

    auto end_time = chrono::steady_clock::now();
    auto duration = chrono::duration_cast<chrono::milliseconds>(end_time - start_time);

    cout << "\n=== import finished ===" << endl;
    cout << "total time cost" << duration.count() << " ms" << endl;

    if (duration.count() > 0) {
        double speed = static_cast<double>(rows.size()) / duration.count();
        cout << "average time: " << speed * 1000 << " lines per second" << endl;
    }

    cout << "按任意键继续...";
    cin.get();

    return 0;
}