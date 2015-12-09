package papaline.concurrent;

import java.util.concurrent.Callable;

public class ArgumentsAwareCallable<V> implements Callable<V> {
    private Callable<V> actualCallable;
    private Object[] arguments;

    public ArgumentsAwareCallable(Callable<V> actualCallable, Object... arguments) {
        this.actualCallable = actualCallable;
        this.arguments = arguments;
    }

    public Object[] getArguments() {
        return this.arguments;
    }

    @Override
    public V call() throws Exception {
        return this.actualCallable.call();
    }
}
