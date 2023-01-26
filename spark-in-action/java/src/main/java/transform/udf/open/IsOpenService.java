package transform.udf.open;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.Calendar;

public class IsOpenService {
    private static Logger log = LoggerFactory.getLogger(IsOpenService.class);

    private IsOpenService() {}

    public static boolean isOpen(String hoursMon, String hoursTue, String hoursWed,
                                 String hoursThu, String hoursFri, String hoursSat,
                                 String hoursSun, Timestamp dateTime) {
        // get the day of the week
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(dateTime.getTime());
        int day = cal.get(Calendar.DAY_OF_WEEK);
        log.trace("Day of the week: {}", day);

        // Assigns the day of the week's hours to the opening hours
        String hours;
        switch (day) {
            case Calendar.MONDAY:
                hours = hoursMon;
                break;

            case Calendar.TUESDAY:
                hours = hoursTue;
                break;

            case Calendar.WEDNESDAY:
                hours = hoursWed;
                break;

            case Calendar.THURSDAY:
                hours = hoursThu;
                break;

            case Calendar.FRIDAY:
                hours = hoursFri;
                break;
            case Calendar.SATURDAY:
                hours = hoursSat;
                break;

            default:
                hours = hoursSun;
        }

        if (hours.compareToIgnoreCase("closed") == 0) {
            return false;
        }

        // Calcuates an event in seconds
        int event = cal.get(Calendar.HOUR_OF_DAY) * 3600
                + cal.get(Calendar.MINUTE) * 60
                + cal.get(Calendar.SECOND);

        String[] ranges = hours.split(" and ");
        for (int i = 0; i < ranges.length; i++) {
            log.trace("Processing range #{}: {}", i, ranges[i]);
            String[] openingHours = ranges[i].split("-");
            // Extract the opening time in seconds
            int start =
                    Integer.valueOf(openingHours[0].substring(0, 2)) * 3600 +
                            Integer.valueOf(openingHours[0].substring(3, 5)) * 60;
            // Extract the closing time in seconds
            int end =
                    Integer.valueOf(openingHours[1].substring(0, 2)) * 3600 +
                            Integer.valueOf(openingHours[1].substring(3, 5)) * 60;
            log.trace("Checking between {} and {}", start, end);
            if (event >= start && event <= end) {
                return true;
            }
        }

        return false;
    }
}
