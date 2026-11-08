"""Run SplitVoteTest scenarios without JUnit; does not replace Maven regression.
Uses the existing compiled shared DTO; all Raft sources are freshly compiled.
Run from repository root: python3 artifacts/evidence/D07/split-vote/run-standalone.py
"""
from pathlib import Path
import subprocess
import tempfile

root = Path.cwd()
source = (root / 'backend-node/src/test/java/vn/nhom6/railway/raft/election/SplitVoteTest.java').read_text()
source = source.replace('import org.junit.jupiter.api.Test;', '')
source = source.replace('import static org.junit.jupiter.api.Assertions.*;', '')
source = source.replace('    @Test\n', '')
# Plain Java assertions throwing AssertionError, not substitute JUnit classes.
checks = '''
    private static void assertTrue(boolean value, String... message) {
        if (!value) throw new AssertionError(java.util.Arrays.toString(message));
    }
    private static void assertEquals(Object expected, Object actual, String... message) {
        boolean same = expected instanceof Number a && actual instanceof Number b
                ? a.longValue() == b.longValue() : java.util.Objects.equals(expected, actual);
        if (!same) throw new AssertionError("expected=" + expected + " actual=" + actual
                + java.util.Arrays.toString(message));
    }
    private static void fail(Throwable failure) { throw new AssertionError(failure); }
    private interface Checked { void run() throws Exception; }
    private static void assertDoesNotThrow(Checked action) {
        try { action.run(); } catch (Exception failure) { throw new AssertionError(failure); }
    }
    public static void main(String[] args) throws Exception {
        SplitVoteTest test = new SplitVoteTest();
        test.twoCandidatesInSameTermCannotBothWinEvenWithDuplicateAndReorderedReplies();
        System.out.println("PASS standalone: same-term competing candidates / duplicate votes");
        test.splitVoteRetriesAtFreshDeadlinesInNewTermAndDiscardsOldGrants();
        System.out.println("PASS standalone: split vote / new-term retry / stale votes");
        test.randomizedPolicyDrawsFreshIntervalForEveryRetry();
        System.out.println("PASS standalone: fresh randomized intervals per retry");
        test.missingQuorumWaitsForDeadlinesInsteadOfSpinningOrCountingOfflinePeers();
        System.out.println("PASS standalone: no quorum / bounded one-shot scheduling");
    }
'''
source = source[:source.rindex('}')] + checks + '}\n'
with tempfile.TemporaryDirectory(prefix='raft-d7-') as directory:
    work = Path(directory)
    test = work / 'SplitVoteTest.java'
    test.write_text(source)
    shared = root / 'shared/target/classes'
    sources = list((root / 'backend-node/src/main/java/vn/nhom6/railway/raft/core').glob('*.java'))
    sources += list((root / 'backend-node/src/main/java/vn/nhom6/railway/raft/election').glob('*.java'))
    subprocess.run(['javac', '--release', '25', '-cp', str(shared), '-d', str(work),
                    *map(str, sources), str(test)], check=True)
    subprocess.run(['java', '-cp', f'{work}:{shared}',
                    'vn.nhom6.railway.raft.election.SplitVoteTest'], check=True)
