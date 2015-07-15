package org.rakam;

import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

/**
 * Created by buremba <Burak Emre Kabakcı> on 13/07/15 12:09.
 */
public class RecipeConfigParser {

    public RecipeConfigParser() {
        Constructor constructor = new Constructor(Recipe.class);
        TypeDescription carDescription = new TypeDescription(Recipe.class);
        constructor.addTypeDescription(carDescription);
        Yaml yaml = new Yaml(constructor);
        Recipe recipe = (Recipe) yaml.load("ecommerce_test.yml");
    }

}
