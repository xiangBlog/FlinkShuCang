package com.xiang.gmall.realtime.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * User: 51728
 * Date: 2022/11/16
 * Desc:
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class KeyWordStats {
    private String keyword;
    private Long ct;
    private String source;
    private String stt;
    private String edt;
    private Long ts;

}
