UPDATE table1
SET n1 = n1 + 1;

DO $$
BEGIN
    IF (SELECT 
            COUNT() 
        FROM table1) <> 100
    THEN
        RAISE 'table1 must have 100 rows';
    END IF;

    IF (SELECT 
            last_value 
        FROM public.seq1) <> 150
    THEN
        RAISE 'sequence seq must have last_value = 150';
    END IF;
END
$$ LANGUAGE plpgsql;

