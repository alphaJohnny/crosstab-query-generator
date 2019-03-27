
SET FOREIGN_KEY_CHECKS=0;
-- ----------------------------
-- Procedure structure for xtab_query generation
-- ----------------------------
DROP PROCEDURE IF EXISTS `xtab`;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `xtab`(`col_name` VARCHAR(256), `col_alias` VARCHAR(256), `col_value` VARCHAR(256), `col_from` VARCHAR(256), `col_where` VARCHAR(256), `col_order` VARCHAR(256), `row_name` VARCHAR(256), `row_from` VARCHAR(1024), `row_order` VARCHAR(256), `row_group` VARCHAR(256) )
    READS SQL DATA
    DETERMINISTIC
    SQL SECURITY INVOKER
    COMMENT 'xtab takes input through params'
BEGIN

    DECLARE `xtab_col_name`  VARCHAR(256)    DEFAULT '';
    DECLARE `xtab_col_alias` VARCHAR(256)    DEFAULT '';
    DECLARE `xtab_query`     VARCHAR(12228)  DEFAULT '';
    DECLARE `done`           BIT(1)         DEFAULT 0;

    DECLARE `column_cursor` CURSOR FOR
        SELECT `temp_col_name`, `temp_col_alias` FROM `xtab_columns`;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET `done` = 1;

    -- We have to use a temporary table here as MySQL doesn't
    -- allow us to declare a cursor in prepared statements
    DROP TABLE IF EXISTS `xtab_columns`;
    SET @column_query := CONCAT('CREATE TEMPORARY TABLE `xtab_columns` ',
                                'SELECT DISTINCT ',
                                '`', `col_name`, '` AS `temp_col_name`, ',
                                '`', `col_alias`, '` AS `temp_col_alias` ',
                                `col_from`);

    PREPARE `column_query` FROM @column_query;
    EXECUTE `column_query`;
    DEALLOCATE PREPARE `column_query`;

    OPEN `column_cursor`;
    column_loop: LOOP
        FETCH `column_cursor` INTO `xtab_col_name`, `xtab_col_alias`;
        IF `done` THEN LEAVE column_loop; END IF;
        SET `xtab_query` = CONCAT(`xtab_query`,
                                  '\tSUM(IF(`', `col_name`, '` = \'',
                                  `xtab_col_name`, '\', ',
                                  `col_value`, ', 0)) AS `',
                                  `xtab_col_alias`, '`,\n');
    END LOOP column_loop;
    CLOSE `column_cursor`;
    DROP TABLE IF EXISTS `xtab_columns`;
-- Fix for multi-var rows
-- expand "," to "`,`" in the row_name variable
SET `row_name` = REPLACE(`row_name`,',','`,`');
    SET `xtab_query` = CONCAT('SELECT `', `row_name`, '`,\n',
                              `xtab_query`, '\t',
                              IF(`col_value` = '1',
                                 'COUNT(*)',
                                 CONCAT('SUM(`', `col_value`, '`)')
                              ),
                              ' AS `total`\n',
                              `row_from`, ' ', `row_where`, ' ', `row_order`, ' ', `row_group`);

    -- Uncomment the following line if you want to see the
    -- generated crosstab query for debugging purposes
    -- SELECT `xtab_query`;

    -- Execute crosstab
    SET @xtab_query = `xtab_query`;
    PREPARE `xtab` FROM @xtab_query;
    EXECUTE `xtab`;
    DEALLOCATE PREPARE `xtab`;
END;;
DELIMITER ;
