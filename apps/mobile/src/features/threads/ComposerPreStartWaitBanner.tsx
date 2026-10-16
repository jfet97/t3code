import { Pressable, View } from "react-native";

import { AppText as Text } from "../../components/ui/AppText";
import { SymbolView } from "../../components/ui/AppSymbol";

/**
 * A first message waiting for pre-start monitors, above the composer. Nothing truncates: the
 * text wraps, and Continue moves to its own line when the deadline and it no longer fit.
 */
export function ComposerPreStartWaitBanner(props: {
  readonly deadlineLabel: string;
  readonly canContinue: boolean;
  readonly continuing: boolean;
  readonly onContinue: () => void;
}) {
  // the time wraps as one word
  const deadline = props.deadlineLabel.replaceAll(" ", "\u00a0");
  return (
    <View className="flex-row flex-wrap items-center gap-x-2 gap-y-1 px-4 pb-2">
      <View className="min-w-0 shrink flex-row items-center gap-2">
        <SymbolView name="clock" size={12} tintColorClassName="accent-foreground-muted" />
        <Text className="shrink text-xs text-foreground-muted">
          Waiting for monitor until <Text className="tabular-nums">{deadline}</Text>
        </Text>
      </View>
      {props.canContinue ? (
        <Pressable
          accessibilityRole="button"
          accessibilityLabel="Continue without monitor"
          disabled={props.continuing}
          onPress={props.onContinue}
          hitSlop={8}
          className="min-h-8 justify-center px-1 active:opacity-70 disabled:opacity-40"
        >
          <Text className="font-t3-medium text-xs text-primary">
            {props.continuing ? "Continuing…" : "Continue without monitor"}
          </Text>
        </Pressable>
      ) : null}
    </View>
  );
}
