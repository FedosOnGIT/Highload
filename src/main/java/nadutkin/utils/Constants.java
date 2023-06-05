package nadutkin.utils;

import nadutkin.database.impl.MemorySegmentDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Constants {
    public static final Long FLUSH_THRESHOLD_BYTES = (long) (1 << 26);
    public static final String REQUEST_PATH = "/v0/entity";
    public static final Integer MAX_FAILS = 100;
    public static final Logger LOG = LoggerFactory.getLogger(MemorySegmentDao.class);

}
