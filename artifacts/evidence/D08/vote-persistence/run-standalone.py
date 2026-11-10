"""Runs VotePersistenceTest scenarios with plain Java checks when JUnit is unavailable.
This is NOT a JUnit/Maven run. Uses existing compiled shared DTO, current Raft sources
and TV4 FakeRaftStorage. Run this script from repository root.
"""
from pathlib import Path
import re
import subprocess
import tempfile

root = Path.cwd()
source = (root / 'backend-node/src/test/java/vn/nhom6/railway/raft/election/VotePersistenceTest.java').read_text()
methods = re.findall(r'@Test\s+void (\w+)\(', source)
source = source.replace('import org.junit.jupiter.api.Test;', '')
source = source.replace('import static org.junit.jupiter.api.Assertions.*;', '')
source = source.replace('    @Test\n', '')
checks = r'''
    private static void assertTrue(boolean value, String... message) {
        if (!value) throw new AssertionError(java.util.Arrays.toString(message));
    }
    private static void assertFalse(boolean value, String... message) { assertTrue(!value, message); }
    private static void assertNull(Object value, String... message) { assertTrue(value == null, message); }
    private static void assertSame(Object expected, Object actual, String... message) {
        assertTrue(expected == actual, message);
    }
    private static void assertEquals(Object expected, Object actual, String... message) {
        boolean same = expected instanceof Number a && actual instanceof Number b
                ? a.longValue() == b.longValue() : java.util.Objects.equals(expected, actual);
        if (!same) throw new AssertionError("expected=" + expected + " actual=" + actual
                + java.util.Arrays.toString(message));
    }
    private static void fail(Object failure) { throw new AssertionError(failure); }
    private interface Checked { void run() throws Exception; }
    private static <T extends Throwable> T assertThrows(Class<T> type, Checked action) {
        try { action.run(); }
        catch (Throwable failure) {
            if (type.isInstance(failure)) return type.cast(failure);
            throw new AssertionError("Unexpected exception", failure);
        }
        throw new AssertionError("Expected exception " + type.getName());
    }
'''
checks += '    public static void main(String[] args) throws Exception {\n'
for method in methods:
    checks += f'        new VotePersistenceTest().{method}();\n'
    checks += f'        System.out.println("PASS standalone: {method}");\n'
checks += '    }\n'
source = source[:source.rindex('}')] + checks + '}\n'
with tempfile.TemporaryDirectory(prefix='raft-d8-') as directory:
    work = Path(directory)
    test = work / 'VotePersistenceTest.java'
    test.write_text(source)
    shared = root / 'shared/target/classes'
    sources = list((root / 'backend-node/src/main/java/vn/nhom6/railway/raft/core').glob('*.java'))
    sources += list((root / 'backend-node/src/main/java/vn/nhom6/railway/raft/election').glob('*.java'))
    sources += [root / 'backend-node/src/test/java/vn/nhom6/railway/raft/replication/FakeRaftStorage.java']
    subprocess.run(['javac', '--release', '25', '-cp', str(shared), '-d', str(work),
                    *map(str, sources), str(test)], check=True)
    subprocess.run(['java', '-cp', f'{work}:{shared}',
                    'vn.nhom6.railway.raft.election.VotePersistenceTest'], check=True)

# D6 regression: execute existing source methods, including explicit parameter cases.
regression = (root / 'backend-node/src/test/java/vn/nhom6/railway/raft/election/VoteSafetyTest.java').read_text()
plain = re.findall(r'@Test\s+void (\w+)\(', regression)
regression = re.sub(r'import org\.junit[^;]+;', '', regression)
regression = regression.replace('import static org.junit.jupiter.api.Assertions.*;', '')
regression = re.sub(r'@(CsvSource|EnumSource|ValueSource)\([^)]*\)', '', regression, flags=re.S)
regression = re.sub(r'@(Test|ParameterizedTest|NullAndEmptySource)\b', '', regression)
calls = [f'{name}()' for name in plain]
for index, term, granted in [(0,0,False),(100,2,False),(2,3,False),(3,3,True),(4,3,True),(1,4,True),(9223372036854775807,3,True)]:
    calls.append(f'comparesLogTermBeforeIndexAndLeavesLogAndCheckpointsUnchanged({index}L,{term}L,{str(granted).lower()})')
for role in ['FOLLOWER', 'CANDIDATE', 'LEADER']:
    calls += [f'higherTermDenialStoresClearedVoteAndStepsDownFromEveryRole(Role.{role})',
              f'higherTermGrantStoresNewVoteAndArmsTimerOnce(Role.{role})']
for role in ['CANDIDATE', 'LEADER']:
    calls.append(f'sameTermRequestCannotStealSelfVoteOrChangeRole(Role.{role})')
for after in ['false', 'true']:
    for term, index, log_term in [(5,3,3),(9,100,2),(9,3,3)]:
        calls.append(f'storageFailureBeforeOrAfterWriteReturnsNoReplyAndPermanentlyClosesHandler({term},{index},{log_term},{after})')
for term, index, log_term in [(-1,0,0),(0,0,0),(9,-1,1),(9,1,-1),(9,0,1),(9,1,0),(9,1,10)]:
    calls.append(f'malformedMetadataCannotMutateStateOrResetTimer({term},{index},{log_term})')
for candidate in ['null', '""', '" "', '"node-6"', '"monitor-1"', '"client-1"']:
    calls.append(f'nonVoterCannotAdvanceTerm({candidate})')
helpers = checks.split('    public static void main')[0]
main = '    public static void main(String[] args) throws Exception {\n'
main += ''.join(f'        new VoteSafetyTest().{call};\n' for call in calls)
main += f'        System.out.println("PASS standalone: {len(calls)} existing D6 VoteSafetyTest scenarios");\n'
main += '    }\n'
regression = regression[:regression.rindex('}')] + helpers + main + '}\n'
with tempfile.TemporaryDirectory(prefix='raft-d8-d6-') as directory:
    work = Path(directory)
    test = work / 'VoteSafetyTest.java'
    test.write_text(regression)
    subprocess.run(['javac', '--release', '25', '-cp', str(shared), '-d', str(work),
                    *map(str, sources), str(test)], check=True)
    subprocess.run(['java', '-cp', f'{work}:{shared}',
                    'vn.nhom6.railway.raft.election.VoteSafetyTest'], check=True)
