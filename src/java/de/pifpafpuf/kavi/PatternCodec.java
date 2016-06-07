package de.pifpafpuf.kavi;

import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import de.pifpafpuf.web.urlparam.ParamCodec;

/**
 * @deprecated this class is not used. Remove at next visit.
 */
public enum PatternCodec implements ParamCodec<Pattern> {
  INSTANCE;

  @Override
  public String asString(Pattern p) {
    return p.pattern();
  }

  /**
   * @throws PatternSyntaxException if the regex cannot be compiled.
   */
  @Override
  public Pattern get(String regex) {
    return Pattern.compile(regex);
  }

}
