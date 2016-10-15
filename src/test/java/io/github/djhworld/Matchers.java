package io.github.djhworld;

import io.github.djhworld.model.RowMutation;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

public class Matchers {
    public static  Matcher<RowMutation> rowMutationMatcher(RowMutation expected) {
        return new TypeSafeMatcher<RowMutation>() {
            @Override
            protected boolean matchesSafely(RowMutation actual) {
                return expected.action.equals(actual.action)
                        && expected.rowKey.equals(actual.rowKey)
                        && expected.columnKey.equals(actual.columnKey)
                        && expected.value.equals(actual.value);
            }

            @Override
            public void describeTo(Description description) {

            }
        };
    }
}
