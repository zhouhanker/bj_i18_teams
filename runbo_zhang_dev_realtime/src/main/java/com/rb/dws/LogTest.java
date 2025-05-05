package com.rb.dws;
//create table dws_traffic_source_keyword_page_view_window(
//          stt string,
//          edt string,
//          cur_date string,
//          keyword string,
//          keyword_count bigint
//        )with(" +
//         'connector' = 'doris',
//         'fenodes' = 'cdh03:8030',
//          'table.identifier' = 'doris_database_v1.dws_traffic_source_keyword_page_view_window',
//          'username' = 'root',
//          'password' = 'root',
//          'sink.properties.format' = 'json',
//          'sink.buffer-count' = '4',
//          'sink.buffer-size' = '4096',
//          'sink.enable-2pc' = 'false',
//          'sink.properties.read_json_by_line' = 'true'
//        )