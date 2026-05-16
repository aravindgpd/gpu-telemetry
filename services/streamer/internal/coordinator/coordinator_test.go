package coordinator

import "testing"

// TestShouldPublishSingleStreamer: a fleet of one owns every row.
func TestShouldPublishSingleStreamer(t *testing.T) {
	c := New(0, 1)
	for row := 0; row < 100; row++ {
		if !c.ShouldPublish(row) {
			t.Errorf("single streamer should own row %d", row)
		}
	}
}

// TestShouldPublishTwoStreamers: streamer 0 takes evens, streamer 1 takes odds.
func TestShouldPublishTwoStreamers(t *testing.T) {
	c0 := New(0, 2)
	c1 := New(1, 2)

	cases := []struct {
		row     int
		want0   bool
		want1   bool
		comment string
	}{
		{0, true, false, "streamer 0 owns row 0"},
		{1, false, true, "streamer 1 owns row 1"},
		{2, true, false, "wraps back"},
		{99, false, true, "odd numbered"},
		{100, true, false, "even numbered"},
	}

	for _, tc := range cases {
		if got := c0.ShouldPublish(tc.row); got != tc.want0 {
			t.Errorf("c0.ShouldPublish(%d) = %v, want %v (%s)", tc.row, got, tc.want0, tc.comment)
		}
		if got := c1.ShouldPublish(tc.row); got != tc.want1 {
			t.Errorf("c1.ShouldPublish(%d) = %v, want %v (%s)", tc.row, got, tc.want1, tc.comment)
		}
	}
}

// TestShouldPublishCoversAllRowsExactlyOnce: across the full fleet, each row
// is owned by exactly one streamer (no row dropped, no row duplicated).
func TestShouldPublishCoversAllRowsExactlyOnce(t *testing.T) {
	const total = 5
	streamers := make([]*Coordinator, total)
	for i := 0; i < total; i++ {
		streamers[i] = New(i, total)
	}

	for row := 0; row < 1000; row++ {
		owners := 0
		for _, c := range streamers {
			if c.ShouldPublish(row) {
				owners++
			}
		}
		if owners != 1 {
			t.Errorf("row %d had %d owners, expected exactly 1", row, owners)
		}
	}
}

// TestPartitionMatchesIndex: each streamer's partition equals its ordinal.
func TestPartitionMatchesIndex(t *testing.T) {
	for i := 0; i < 10; i++ {
		c := New(i, 10)
		if got := c.Partition(); got != int32(i) {
			t.Errorf("New(%d, 10).Partition() = %d, want %d", i, got, i)
		}
	}
}
