import java.nio.file.*;
import java.sql.*;
import java.time.Instant;
import java.util.*;
import vn.nhom6.railway.booking.*;
import vn.nhom6.railway.persistence.*;
import vn.nhom6.railway.raft.core.RaftLogEntry;

/** Isolated JDBC durability fixture; deliberately NOT a Raft consensus test. */
class JdbcDurabilityProbe {
    public static void main(String[] args) throws Exception {
        String schema=args[1];
        if (!schema.matches("railway_test_[a-z0-9]+")) throw new IllegalArgumentException("test schema only");
        String user=System.getenv("RAILWAY_TEST_DB_USER"), password=System.getenv("RAILWAY_TEST_DB_PASSWORD");
        var cfg=new DbConfig("127.0.0.1",5433,"railway_node1?currentSchema="+schema,user,password);
        var factory=new ConnectionFactory(cfg);
        String id="11111111-1111-1111-1111-111111111111";
        if (args[0].equals("prepare")) {
            try (var c=factory.getConnection(); var s=c.createStatement()) {
                s.execute("CREATE SCHEMA "+schema);
                for (String migration:List.of("V1__schema.sql","V2__seed.sql"))
                    s.execute(Files.readString(Path.of("database/migration",migration)));
            }
            var storage=new JdbcRaftStorage(factory);
            storage.saveTermAndVote(1,"node-1");
            String json="{\"requestId\":\""+id+"\",\"tripId\":\"SE-001\",\"seatId\":\"G02\",\"passengerName\":\"Durability Demo\",\"ticketId\":\"durability-ticket\",\"issuedAt\":\"2026-11-11T00:00:00Z\"}";
            storage.replaceSuffix(1,List.of(new RaftLogEntry(1,1,"BOOK_TICKET",json)));
            // Controlled fixture delivery only, no claim of quorum commit.
            new JdbcCommittedCommandApplier(factory).applyCommitted(1,new BookingCommand.BookTicket(
                id,"SE-001","G02","Durability Demo","durability-ticket",Instant.parse("2026-11-11T00:00:00Z")));
        }
        var repository=new JdbcBookingRepository(factory);
        var expected=ApplyResult.bookConfirmed(id,"durability-ticket",1);
        if (!repository.findResultByRequestId(id).orElseThrow().equals(expected)
                || repository.lastApplied()!=1 || !repository.readSeatSnapshot().get("G02").equals("BOOKED"))
            throw new AssertionError("Durable JDBC result/checkpoint/seat differs");
        try(var c=factory.getConnection();var s=c.createStatement();var rs=s.executeQuery(
                "SELECT (SELECT count(*) FROM tickets) AS tickets,(SELECT count(*) FROM request_results) AS results,"
                +"(SELECT last_applied FROM raft_apply_progress) AS last_applied")) {
            rs.next(); if(rs.getInt(1)!=1||rs.getInt(2)!=1||rs.getLong(3)!=1)throw new AssertionError("counts differ");
            System.out.println(args[0]+": JDBC SQL tickets=1, request_results=1, last_applied=1; requestId="+id+", G02 BOOKED, ticket=durability-ticket; controlled fixture, NOT Raft");
        }
        if(args[0].equals("verify"))try(var c=factory.getConnection();var s=c.createStatement()){
            s.execute("DROP SCHEMA "+schema+" CASCADE");
        }
    }
}
