package voldemort.performance.benchmark;

import org.apache.log4j.Logger;

import voldemort.performance.benchmark.generator.UniformIntegerGenerator;

public class TemporaryUniformIntegerGeneratorThenSingleKey extends UniformIntegerGenerator {

    public TemporaryUniformIntegerGeneratorThenSingleKey(int lb, int ub, int fixedkey) {
        super(lb, ub);
        this.started_at = System.currentTimeMillis();
        this.fixedkey = fixedkey;
    }

    @Override
    public int nextInt() {
        int key = fixedkey;
        if(!switched_to_fixed) {
            if((System.currentTimeMillis() - started_at) > uniformDurationMilis) {
                switched_to_fixed = true;
                Logger.getLogger(TemporaryUniformIntegerGeneratorThenSingleKey.class)
                      .info("SWITCHING TO SINGLE KEY LOAD");
            } else {
                key = super.nextInt();
            }

        }
        return key;
    }

    private final int uniformDurationMilis = 3 * 60 * 1000;
    private boolean switched_to_fixed = false;
    private long started_at;
    private int fixedkey;
}
