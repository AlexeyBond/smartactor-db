package info.smart_tools.smartactors.actors.db_accessor.storage;

import java.util.regex.Pattern;

public interface FieldPath {
    Pattern validationPattern = Pattern.compile("^([a-zA-Z_][a-zA-Z0-9_]*)((\\.([a-zA-Z_][a-zA-Z0-9_]*))|(\\[[0-9]+\\]))*$");
    Pattern splitPattern = Pattern.compile("[\\[\\]\\.]+");

    static boolean isValid(String path) {
        return validationPattern.matcher(path).matches();
    }

    static String[] splitParts(String path) {
        return splitPattern.split(path);
    }

    String getSQLRepresentation();
}
